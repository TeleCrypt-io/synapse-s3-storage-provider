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
import os
import threading

from six import string_types

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

# Synapse's TeleCrypt runtime uses the fixed disk-backed staging mount for
# upload payloads. The provider deliberately rejects the persistent
# compatibility path and the ambient process temporary directory.
MEDIA_STAGING_ROOT = "/staging"
MEDIA_STAGING_DIRECTORY = "/staging/tmp"

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


class S3StorageProviderBackend(StorageProvider):
    """
    Args:
        hs (HomeServer)
        config: The config returned by `parse_config`
    """

    def __init__(self, hs, config):
        self._module_api: ModuleApi = hs.get_module_api()
        self.bucket = config["bucket"]
        self.extra_args = {}
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
        if not upload_path or not isinstance(upload_path, string_types):
            raise ValueError(
                "Synapse did not provide the temporary source path for media upload"
            )
        upload_path = _validated_upload_source(upload_path)

        return await self._module_api.defer_to_threadpool(
            self._s3_pool,
            _put_object_from_file,
            self._get_s3_client(),
            self.bucket,
            path,
            upload_path,
            self.extra_args,
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
        run_in_background(
            self._module_api.defer_to_threadpool,
            self._s3_pool,
            s3_download_task,
            self._get_s3_client(),
            self.bucket,
            path,
            self.extra_args,
            d,
        )

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
            if not isinstance(config[key], string_types) or not config[key]:
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


def _put_object_from_file(s3_client, bucket, key, source_path, extra_args):
    """Upload one file with one ordinary S3 PutObject request.

    ``upload_file`` is deliberately not used here: boto3's managed transfer
    implementation may switch to multipart uploads for larger files. The
    caller has already validated the source path and canonical key through
    Synapse's storage-provider interface.
    """

    source_path = _validated_upload_source(source_path)
    with open(source_path, "rb") as source:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=source,
            **extra_args,
        )


def _validated_upload_source(source_path):
    """Return a real temporary source path beneath the fixed staging directory."""

    if not isinstance(source_path, string_types):
        raise ValueError("Synapse temporary media source path must be a string")

    staging_root = os.path.realpath(MEDIA_STAGING_ROOT)
    staging_directory = os.path.realpath(MEDIA_STAGING_DIRECTORY)
    source = os.path.realpath(source_path)

    try:
        staging_is_valid = (
            os.path.commonpath((staging_root, staging_directory)) == staging_root
        )
        source_is_staged = (
            os.path.commonpath((staging_directory, source)) == staging_directory
        )
    except ValueError:
        staging_is_valid = False
        source_is_staged = False

    if not staging_is_valid or not source_is_staged:
        raise ValueError(
            "Synapse temporary media source must be beneath /staging/tmp"
        )
    if not os.path.isfile(source):
        raise ValueError("Synapse temporary media source does not exist")

    return source


def _delete_object(s3_client, bucket, key):
    """Delete one exact S3 object, treating an absent object as success."""

    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in ("404", "NoSuchKey", "NotFound"):
            raise


def s3_download_task(s3_client, bucket, key, extra_args, deferred):
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
        if "SSECustomerKey" in extra_args and "SSECustomerAlgorithm" in extra_args:
            resp = s3_client.get_object(
                Bucket=bucket,
                Key=key,
                SSECustomerKey=extra_args["SSECustomerKey"],
                SSECustomerAlgorithm=extra_args["SSECustomerAlgorithm"],
            )
        else:
            resp = s3_client.get_object(Bucket=bucket, Key=key)

    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey",):
            logger.info("Media %s not found in S3", key)
            reactor.callFromThread(deferred.callback, None)
            return

        reactor.callFromThread(deferred.errback, Failure())
        return

    producer = _S3Responder()
    reactor.callFromThread(deferred.callback, producer)
    _stream_to_producer(reactor, producer, resp["Body"], timeout=90.0)


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

            reactor.callFromThread(producer._write, chunk)

    except Exception:
        reactor.callFromThread(producer._error, Failure())
    finally:
        reactor.callFromThread(producer._finish)
        if body:
            body.close()


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

    def write_to_consumer(self, consumer):
        """See Responder.write_to_consumer
        """
        self.consumer = consumer
        # We are a IPushProducer, so we start producing immediately until we
        # get a pauseProducing or stopProducing
        consumer.registerProducer(self, True)
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
        self.stop_event.set()
        self.wakeup_event.set()
        if not self.deferred.called:
            self.deferred.errback(Exception("Consumer ask to stop producing"))

    def _write(self, chunk):
        """Writes the chunk of data to consumer. Called by _S3DownloadThread.
        """
        if self.consumer and not self.stop_event.is_set():
            self.consumer.write(chunk)

    def _error(self, failure):
        """Called when a fatal error occured while getting data. Called by
        _S3DownloadThread.
        """
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None

        if not self.deferred.called:
            self.deferred.errback(failure)

    def _finish(self):
        """Called when there is no more data to write. Called by _S3DownloadThread.
        """
        if self.consumer:
            self.consumer.unregisterProducer()
            self.consumer = None

        if not self.deferred.called:
            self.deferred.callback(None)


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
