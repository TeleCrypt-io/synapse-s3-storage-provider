# -*- coding: utf-8 -*-
# Copyright 2018 New Vector Ltd
# Copyright 2021 The Matrix.org Foundation C.I.C.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import threading

import boto3
import botocore
from botocore.config import Config

from twisted.internet import defer, reactor
from twisted.python.failure import Failure
from twisted.python.threadpool import ThreadPool

from synapse.logging.context import make_deferred_yieldable
from synapse.module_api import ModuleApi, run_in_background
from synapse.rest.media.v1._base import Responder
from synapse.rest.media.v1.storage_provider import StorageProvider

logger = logging.getLogger("synapse.s3")


# Chunk size to use when reading from s3 connection in bytes
READ_CHUNK_SIZE = 16 * 1024

_REQUIRED_CONFIG_KEYS = frozenset(
    {
        "bucket",
        "endpoint_url",
        "region_name",
        "access_key_id",
        "secret_access_key",
    }
)

_CANONICAL_ENDPOINT_URL = "https://sss.telecrypt.io"
_S3_NOT_FOUND_CODES = frozenset(("404", "NoSuchKey", "NotFound"))


class S3StorageProviderBackend(StorageProvider):
    """
    Args:
        hs (HomeServer)
        config: The config returned by `parse_config`
    """

    def __init__(self, hs, config):
        self._module_api: ModuleApi = hs.get_module_api()
        self.bucket = config["bucket"]
        self.api_kwargs = {}

        self.api_kwargs["region_name"] = config["region_name"]
        self.api_kwargs["endpoint_url"] = config["endpoint_url"]
        self.api_kwargs["aws_access_key_id"] = config["access_key_id"]
        self.api_kwargs["aws_secret_access_key"] = config["secret_access_key"]
        self.api_kwargs["config"] = Config()

        self._s3_client = None
        self._s3_client_lock = threading.Lock()

        self._s3_pool = ThreadPool(name="s3-pool", maxthreads=40)
        self._s3_pool.start()

        # Manually stop the thread pool on shutdown. If we don't do this then
        # stopping Synapse takes an extra ~30s as Python waits for the threads
        # to exit.
        reactor.addSystemEventTrigger(
            "during", "shutdown", self._s3_pool.stop,
        )

    def _get_s3_client(self):
        # this method is designed to be thread-safe, so that we can share a
        # single boto3 client across multiple threads.
        #
        # (XXX: is creating a client actually a blocking operation, or could we do
        # this on the main thread, to simplify all this?)

        # first of all, do a fast lock-free check
        s3 = self._s3_client
        if s3:
            return s3

        # no joy, grab the lock and repeat the check
        with self._s3_client_lock:
            s3 = self._s3_client
            if not s3:
                b3_session = boto3.session.Session()
                self._s3_client = s3 = b3_session.client("s3", **self.api_kwargs)
            return s3

    @property
    def supports_deletion(self):
        return True

    async def store_file(self, path, file_info):
        """See StorageProvider.store_file"""

        upload_path = getattr(file_info, "upload_path", None)
        if not upload_path or not isinstance(upload_path, str):
            raise ValueError(
                "Synapse did not provide the temporary source path for media upload"
            )

        return await self._module_api.defer_to_threadpool(
            self._s3_pool,
            _upload_file,
            self._get_s3_client(),
            self.bucket,
            path,
            upload_path,
        )

    async def delete(self, path, file_info):
        """Delete the exact object represented by ``path``.

        The storage-provider interface intentionally supplies the canonical
        Synapse path.  Callers never provide an S3 key or prefix.
        """

        return await self._module_api.defer_to_threadpool(
            self._s3_pool,
            _delete_object,
            self._get_s3_client(),
            self.bucket,
            path,
        )

    async def fetch(self, path, file_info):
        """See StorageProvider.fetch"""
        d = defer.Deferred()

        # Don't await this directly, as it will resolve only once the streaming
        # download from S3 is concluded. Before that happens, we want to pass
        # execution back to Synapse to stream the file's chunks.
        #
        # We do, however, need to wrap in `run_in_background` to ensure that the
        # coroutine returned by `defer_to_threadpool` is used, and therefore
        # actually run.
        download_deferred = run_in_background(
            self._module_api.defer_to_threadpool,
            self._s3_pool,
            s3_download_task,
            self._get_s3_client(),
            self.bucket,
            path,
            d,
        )
        download_deferred.addErrback(_forward_download_failure, d)

        # DO await on `d`, as it will resolve once a connection to S3 has been
        # opened. We only want to return to Synapse once we can start streaming
        # chunks.
        return await make_deferred_yieldable(d)

    @staticmethod
    def parse_config(config):
        """Called on startup to parse config supplied. This should parse
        the config and raise if there is a problem.

        The returned value is passed into the constructor.

        The TeleCrypt runtime deliberately supports one exact configuration
        shape. Optional legacy settings can change the bucket contract or
        introduce capabilities that the deployment does not verify.
        """
        unexpected_keys = set(config) - _REQUIRED_CONFIG_KEYS
        missing_keys = _REQUIRED_CONFIG_KEYS - set(config)
        if unexpected_keys or missing_keys:
            raise ValueError(
                "S3 provider config must contain exactly bucket, endpoint_url, "
                "region_name, access_key_id, and secret_access_key; "
                "unexpected=%s missing=%s"
                % (sorted(unexpected_keys), sorted(missing_keys))
            )

        for key in _REQUIRED_CONFIG_KEYS:
            if not isinstance(config[key], str) or not config[key]:
                raise ValueError(
                    "S3 provider config %s must be a non-empty string" % key
                )

        if config["endpoint_url"] != _CANONICAL_ENDPOINT_URL:
            raise ValueError(
                "S3 provider endpoint_url must be %s" % _CANONICAL_ENDPOINT_URL
            )

        return {
            "bucket": config["bucket"],
            "endpoint_url": config["endpoint_url"],
            "region_name": config["region_name"],
            "access_key_id": config["access_key_id"],
            "secret_access_key": config["secret_access_key"],
        }


def _upload_file(s3_client, bucket, key, source_path):
    """Upload the temporary file using boto3's standard managed transfer."""

    if not isinstance(source_path, str):
        raise ValueError("Synapse temporary media source path must be a string")
    s3_client.upload_file(source_path, bucket, key)


def _delete_object(s3_client, bucket, key):
    """Delete one exact S3 object, treating an absent object as success."""

    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as error:
        if not _is_s3_not_found_error(error):
            raise


def _is_s3_not_found_error(error):
    """Return whether an S3 operation reported an absent object."""

    return error.response.get("Error", {}).get("Code") in _S3_NOT_FOUND_CODES


def _callback_download_deferred(deferred, result):
    """Settle the fetch Deferred once, preserving any earlier result."""

    if not deferred.called:
        deferred.callback(result)


def _errback_download_deferred(deferred, failure):
    """Fail the fetch Deferred once, preserving any earlier result."""

    if not deferred.called:
        deferred.errback(failure)


def _forward_download_failure(failure, deferred):
    """Forward a worker/background failure unless fetch already settled."""

    _errback_download_deferred(deferred, failure)
    return None


def _append_failure_cause(failure, additional_error):
    """Keep an additional operation or cleanup failure on the same chain."""

    current_error = failure.value
    while current_error.__cause__ is not None:
        current_error = current_error.__cause__
    current_error.__cause__ = additional_error


def s3_download_task(s3_client, bucket, key, deferred):
    """Attempts to download a file from S3.

    Args:
        s3_client: boto3 s3 client
        bucket (str): The S3 bucket which may have the file
        key (str): The key of the file
        deferred (Deferred[_S3Responder|None]): If file exists
            resolved with an _S3Responder instance, if it doesn't
            exist then resolves with None.

    Returns:
        A deferred which resolves to an _S3Responder if the file exists.
        Otherwise the deferred fails.
    """
    logger.info("Fetching %s from S3", key)

    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)

    except botocore.exceptions.ClientError as e:
        if _is_s3_not_found_error(e):
            logger.info("Media %s not found in S3", key)
            reactor.callFromThread(_callback_download_deferred, deferred, None)
            return

        reactor.callFromThread(_errback_download_deferred, deferred, Failure())
        return
    except Exception:
        reactor.callFromThread(_errback_download_deferred, deferred, Failure())
        return

    body = None
    try:
        body = resp["Body"]
        if not callable(getattr(body, "read", None)) or not callable(
            getattr(body, "close", None)
        ):
            raise ValueError("S3 get_object response has an invalid Body")
    except Exception:
        failure = Failure()
        try:
            if body is not None:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
        except Exception as close_error:
            _append_failure_cause(failure, close_error)
        reactor.callFromThread(_errback_download_deferred, deferred, failure)
        return

    producer = _S3Responder()
    reactor.callFromThread(_callback_download_deferred, deferred, producer)
    _stream_to_producer(reactor, producer, body, timeout=90.0)


def _stream_to_producer(reactor, producer, body, status=None, timeout=None):
    """Streams a file like object to the producer.

    Correctly handles producer being paused/resumed/stopped.

    Args:
        reactor
        producer (_S3Responder): Producer object to stream results to
        body (file like): The object to read from
        status (_ProducerStatus|None): Used to track whether we're currently
            paused or not. Used for testing
        timeout (float|None): Timeout in seconds to wait for consume to resume
            after being paused
    """

    # Set when we should be producing, cleared when we are paused
    wakeup_event = producer.wakeup_event

    # Set if we should stop producing forever
    stop_event = producer.stop_event

    if not status:
        status = _ProducerStatus()

    try:
        while not stop_event.is_set():
            # We wait for the producer to signal that the consumer wants
            # more data (or we should abort)
            if not wakeup_event.is_set():
                status.set_paused(True)
                ret = wakeup_event.wait(timeout)
                if not ret:
                    raise Exception("Timed out waiting to resume")
                status.set_paused(False)

            # Check if we were woken up so that we abort the download
            if stop_event.is_set():
                return

            chunk = body.read(READ_CHUNK_SIZE)
            if not chunk:
                return

            write_done = threading.Event()

            def write_chunk():
                try:
                    producer._write(chunk)
                finally:
                    write_done.set()

            reactor.callFromThread(write_chunk)
            write_done.wait()

    except Exception:
        producer._record_failure(Failure())
    finally:
        try:
            if body is not None:
                body.close()
        except Exception:
            producer._record_failure(Failure())
        if producer._has_failure():
            reactor.callFromThread(producer._error)
        else:
            reactor.callFromThread(producer._finish)


class _S3Responder(Responder):
    """A Responder for S3. Created by _S3DownloadThread
    """

    def __init__(self):
        # Triggered by responder when more data has been requested (or
        # stop_event has been triggered)
        self.wakeup_event = threading.Event()
        # Trigered by responder when we should abort the download.
        self.stop_event = threading.Event()

        # The consumer we're registered to
        self.consumer = None

        # The deferred returned by write_to_consumer, which should resolve when
        # all the data has been written (or there has been a fatal error).
        self.deferred = defer.Deferred()
        self._failure = None
        self._failure_lock = threading.Lock()
        self._producer_registered = False

    def _record_failure(self, failure):
        """Record one failure while preserving any earlier failure."""

        with self._failure_lock:
            if self._failure is None:
                self._failure = failure
            else:
                _append_failure_cause(self._failure, failure.value)

    def _has_failure(self):
        with self._failure_lock:
            return self._failure is not None

    def _get_failure(self):
        with self._failure_lock:
            return self._failure

    def write_to_consumer(self, consumer):
        """See Responder.write_to_consumer
        """
        self.consumer = consumer
        # We are a IPushProducer, so we start producing immediately until we
        # get a pauseProducing or stopProducing
        try:
            consumer.registerProducer(self, True)
        except Exception:
            failure = Failure()
            self._record_failure(failure)
            self.consumer = None
            self.stop_event.set()
            self.wakeup_event.set()
            if not self.deferred.called:
                self.deferred.errback(failure)
            return make_deferred_yieldable(self.deferred)
        self._producer_registered = True
        self.wakeup_event.set()
        return make_deferred_yieldable(self.deferred)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_event.set()
        self.wakeup_event.set()

    def resumeProducing(self):
        """See IPushProducer.resumeProducing
        """
        # The consumer is asking for more data, signal _S3DownloadThread
        self.wakeup_event.set()

    def pauseProducing(self):
        """See IPushProducer.stopProducing
        """
        self.wakeup_event.clear()

    def stopProducing(self):
        """See IPushProducer.stopProducing
        """
        # The consumer wants no more data ever, signal _S3DownloadThread
        if not self.stop_event.is_set():
            self._record_failure(Failure(Exception("Consumer ask to stop producing")))
        self.stop_event.set()
        self.wakeup_event.set()

    def _write(self, chunk):
        """Writes the chunk of data to consumer. Called by _S3DownloadThread.
        """
        if self.consumer and not self.stop_event.is_set():
            try:
                self.consumer.write(chunk)
            except Exception:
                self._record_failure(Failure())
                self.stop_event.set()
                self.wakeup_event.set()

    def _error(self):
        """Called when a fatal error occured while getting data. Called by
        _S3DownloadThread.
        """
        failure = self._get_failure()
        if self.consumer is not None and self._producer_registered:
            try:
                self.consumer.unregisterProducer()
            except Exception as cleanup_error:
                if failure is None:
                    failure = Failure()
                else:
                    _append_failure_cause(failure, cleanup_error)
            finally:
                self.consumer = None
                self._producer_registered = False
        else:
            self.consumer = None

        if failure is not None and not self.deferred.called:
            self.deferred.errback(failure)

    def _finish(self):
        """Called when there is no more data to write. Called by _S3DownloadThread.
        """
        failure = self._get_failure()
        if self.consumer is not None and self._producer_registered:
            try:
                self.consumer.unregisterProducer()
            except Exception:
                cleanup_error = Failure()
                if failure is None:
                    failure = cleanup_error
                else:
                    _append_failure_cause(failure, cleanup_error.value)
            finally:
                self.consumer = None
                self._producer_registered = False
        else:
            self.consumer = None

        if not self.deferred.called:
            if failure is None:
                self.deferred.callback(None)
            else:
                self.deferred.errback(failure)


class _ProducerStatus(object):
    """Used to track whether the s3 download thread is currently paused
    waiting for consumer to resume. Used for testing.
    """

    def __init__(self):
        self.is_paused = threading.Event()
        self.is_paused.clear()

    def wait_until_paused(self, timeout=None):
        is_paused = self.is_paused.wait(timeout)
        if not is_paused:
            raise Exception("Timed out waiting")

    def set_paused(self, paused):
        if paused:
            self.is_paused.set()
        else:
            self.is_paused.clear()
