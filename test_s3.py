# -*- coding: utf-8 -*-
# Copyright 2018 New Vector Ltd
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

import os
from queue import Queue
from tempfile import TemporaryDirectory
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from twisted.internet import defer, reactor
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial import unittest

from s3_storage_provider import (
    S3StorageProviderBackend,
    _delete_object,
    _ProducerStatus,
    _put_object_from_file,
    _S3Responder,
    _open_validated_upload_source,
    _stream_to_producer,
    _validated_upload_source,
    s3_download_task,
)


class S3ObjectOperationTestCase(unittest.TestCase):
    def test_store_uses_one_put_object_from_the_given_source(self):
        client = Mock()
        observed = {}

        def put_object(**kwargs):
            observed.update(kwargs)
            observed["body"] = kwargs["Body"].read()

        client.put_object.side_effect = put_object

        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            staging_media = os.path.join(staging, "media")
            os.makedirs(staging_tmp)
            os.makedirs(staging_media)
            source_path = os.path.join(staging_tmp, "upload")
            with open(source_path, "wb") as source:
                source.write(b"temporary media")

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                _put_object_from_file(
                    client,
                    "media-bucket",
                    "media/local/abc",
                    source_path,
                )

            self.assertEqual(
                os.path.commonpath((staging_tmp, source_path)), staging_tmp
            )
            self.assertNotEqual(
                os.path.commonpath((staging_media, source_path)), staging_media
            )

        self.assertEqual(observed["Bucket"], "media-bucket")
        self.assertEqual(observed["Key"], "media/local/abc")
        self.assertEqual(observed["body"], b"temporary media")
        client.put_object.assert_called_once()
        client.upload_file.assert_not_called()

    def test_exact_128_mib_uses_one_non_multipart_put_object(self):
        client = Mock()
        observed = {}
        exact_size = 128 * 1024 * 1024

        def put_object(**kwargs):
            observed["size"] = os.fstat(kwargs["Body"].fileno()).st_size

        client.put_object.side_effect = put_object

        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            os.makedirs(staging_tmp)
            source_path = os.path.join(staging_tmp, "exact-128-mib")
            with open(source_path, "wb") as source:
                source.truncate(exact_size)

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                _put_object_from_file(
                    client,
                    "media-bucket",
                    "media/local/exact-128-mib",
                    source_path,
                )

        self.assertEqual(observed["size"], exact_size)
        client.put_object.assert_called_once()
        client.upload_file.assert_not_called()
        client.create_multipart_upload.assert_not_called()
        client.upload_part.assert_not_called()
        client.complete_multipart_upload.assert_not_called()
        client.abort_multipart_upload.assert_not_called()

    def test_retry_after_post_commit_error_reuses_exact_key(self):
        client = Mock()
        attempts = []

        def put_object(**kwargs):
            attempts.append((kwargs["Bucket"], kwargs["Key"], kwargs["Body"].read()))
            if len(attempts) == 1:
                # The object may already be durable when the response is lost. The
                # caller's safe retry must therefore use the same exact key.
                raise RuntimeError("response lost after object commit")

        client.put_object.side_effect = put_object

        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            os.makedirs(staging_tmp)
            source_path = os.path.join(staging_tmp, "retry-upload")
            with open(source_path, "wb") as source:
                source.write(b"retry-safe media")

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                with self.assertRaisesRegex(RuntimeError, "response lost"):
                    _put_object_from_file(
                        client, "media-bucket", "media/local/retry", source_path
                    )
                _put_object_from_file(
                    client, "media-bucket", "media/local/retry", source_path
                )

        self.assertEqual(attempts, [
            ("media-bucket", "media/local/retry", b"retry-safe media"),
            ("media-bucket", "media/local/retry", b"retry-safe media"),
        ])
        client.upload_file.assert_not_called()
        client.create_multipart_upload.assert_not_called()
        client.complete_multipart_upload.assert_not_called()
        client.abort_multipart_upload.assert_not_called()

    def test_rejects_source_in_persistent_media_compatibility_path(self):
        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            staging_media = os.path.join(staging, "media")
            os.makedirs(staging_tmp)
            os.makedirs(staging_media)
            source_path = os.path.join(staging_media, "upload")
            with open(source_path, "wb") as source:
                source.write(b"must not upload")

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                with self.assertRaisesRegex(ValueError, "beneath /staging/tmp"):
                    _put_object_from_file(
                        Mock(), "media-bucket", "media/local/abc", source_path
                    )

    def test_rejects_source_outside_staging_mount(self):
        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            os.makedirs(staging_tmp)
            source_path = os.path.join(root, "ambient-upload")
            with open(source_path, "wb") as source:
                source.write(b"must not upload")

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                with self.assertRaisesRegex(ValueError, "beneath /staging/tmp"):
                    _validated_upload_source(source_path)

    def test_rejects_staging_path_prefix_lookalike(self):
        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            lookalike = os.path.join(staging, "tmp2")
            os.makedirs(staging_tmp)
            os.makedirs(lookalike)
            source_path = os.path.join(lookalike, "upload")
            with open(source_path, "wb") as source:
                source.write(b"must not upload")

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                with self.assertRaisesRegex(ValueError, "beneath /staging/tmp"):
                    _validated_upload_source(source_path)

    def test_rejects_symlink_to_source_outside_staging_mount(self):
        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            os.makedirs(staging_tmp)
            outside_path = os.path.join(root, "outside-upload")
            with open(outside_path, "wb") as source:
                source.write(b"must not upload")
            source_path = os.path.join(staging_tmp, "upload")
            os.symlink(outside_path, source_path)

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                with self.assertRaisesRegex(ValueError, "beneath /staging/tmp"):
                    _validated_upload_source(source_path)

    def test_rejects_source_replaced_with_external_symlink_before_open(self):
        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            os.makedirs(staging_tmp)
            outside_path = os.path.join(root, "outside-upload")
            with open(outside_path, "wb") as source:
                source.write(b"must not upload")
            source_path = os.path.join(staging_tmp, "upload")
            with open(source_path, "wb") as source:
                source.write(b"temporary media")

            original_open = os.open
            replaced = False

            def replace_before_open(path, flags):
                nonlocal replaced
                if path == source_path and not replaced:
                    os.unlink(source_path)
                    os.symlink(outside_path, source_path)
                    replaced = True
                return original_open(path, flags)

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ), patch("s3_storage_provider.os.open", side_effect=replace_before_open):
                with self.assertRaisesRegex(ValueError, "beneath /staging/tmp"):
                    _put_object_from_file(
                        Mock(), "media-bucket", "media/local/abc", source_path
                    )

            self.assertTrue(replaced)

    def test_preserves_validation_and_descriptor_close_failures(self):
        source_path = "/staging/tmp/upload"
        with patch(
            "s3_storage_provider.os.open", return_value=42
        ), patch(
            "s3_storage_provider.os.path.realpath",
            side_effect=[source_path, "/staging/tmp"],
        ), patch(
            "s3_storage_provider.os.fstat",
            side_effect=RuntimeError("validation failed"),
        ), patch(
            "s3_storage_provider.os.close",
            side_effect=OSError("close failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "validation failed") as raised:
                _open_validated_upload_source(source_path)

        self.assertIsNotNone(raised.exception.__cause__)
        self.assertEqual(str(raised.exception.__cause__), "close failed")

    def test_delete_uses_the_exact_key(self):
        client = Mock()

        _delete_object(client, "media-bucket", "media/local/abc")

        client.delete_object.assert_called_once_with(
            Bucket="media-bucket", Key="media/local/abc"
        )

    def test_delete_treats_absent_object_as_success(self):
        for error_code in ("404", "NoSuchKey", "NotFound"):
            with self.subTest(error_code=error_code):
                client = Mock()
                client.delete_object.side_effect = ClientError(
                    {"Error": {"Code": error_code}}, "DeleteObject"
                )

                _delete_object(client, "media-bucket", "media/local/abc")

    def test_delete_propagates_other_errors(self):
        client = Mock()
        client.delete_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "DeleteObject"
        )

        with self.assertRaises(ClientError):
            _delete_object(client, "media-bucket", "media/local/abc")


class S3BackendWiringTestCase(unittest.TestCase):
    """Check that the Synapse-facing backend preserves canonical media paths."""

    def _backend(self, module_api):
        backend = object.__new__(S3StorageProviderBackend)
        backend._module_api = module_api
        backend._s3_client = object()
        backend._s3_pool = object()
        backend.bucket = "media-bucket"
        return backend

    @defer.inlineCallbacks
    def test_store_file_passes_temporary_source_and_exact_key(self):
        observed = []

        async def defer_to_threadpool(*args):
            observed.append(args)

        module_api = SimpleNamespace(defer_to_threadpool=defer_to_threadpool)
        backend = self._backend(module_api)

        with TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            staging_tmp = os.path.join(staging, "tmp")
            os.makedirs(staging_tmp)
            source_path = os.path.join(staging_tmp, "upload")
            with open(source_path, "wb") as source:
                source.write(b"backend wiring")

            with patch(
                "s3_storage_provider.MEDIA_STAGING_ROOT", staging
            ), patch(
                "s3_storage_provider.MEDIA_STAGING_DIRECTORY", staging_tmp
            ):
                result = yield defer.ensureDeferred(
                    backend.store_file(
                        "local_content/aa/bb/exact-key",
                        SimpleNamespace(upload_path=source_path),
                    )
                )

        self.assertIsNone(result)
        self.assertEqual(len(observed), 1)
        self.assertIs(observed[0][0], backend._s3_pool)
        self.assertIs(observed[0][1], _put_object_from_file)
        self.assertIs(observed[0][2], backend._s3_client)
        self.assertEqual(
            observed[0][3:],
            (
                "media-bucket",
                "local_content/aa/bb/exact-key",
                source_path,
            ),
        )

    @defer.inlineCallbacks
    def test_delete_passes_exact_key_without_prefix_or_headers(self):
        observed = []

        async def defer_to_threadpool(*args):
            observed.append(args)

        module_api = SimpleNamespace(defer_to_threadpool=defer_to_threadpool)
        backend = self._backend(module_api)
        yield defer.ensureDeferred(
            backend.delete(
                "local_content/aa/bb/exact-key",
                SimpleNamespace(),
            )
        )

        self.assertEqual(len(observed), 1)
        self.assertIs(observed[0][0], backend._s3_pool)
        self.assertIs(observed[0][1], _delete_object)
        self.assertIs(observed[0][2], backend._s3_client)
        self.assertEqual(
            observed[0][3:], ("media-bucket", "local_content/aa/bb/exact-key")
        )

    @defer.inlineCallbacks
    def test_fetch_passes_exact_key_without_prefix(self):
        observed = []

        def run_in_background(function, *args):
            observed.append((function, args))
            args[-1].callback(None)
            return defer.succeed(None)

        module_api = SimpleNamespace(defer_to_threadpool=Mock())
        backend = self._backend(module_api)
        with patch("s3_storage_provider.run_in_background", run_in_background):
            result = yield defer.ensureDeferred(
                backend.fetch(
                    "local_content/aa/bb/exact-key",
                    SimpleNamespace(),
                )
            )

        self.assertIsNone(result)
        self.assertEqual(len(observed), 1)
        self.assertIs(observed[0][0], module_api.defer_to_threadpool)
        self.assertIs(observed[0][1][0], backend._s3_pool)
        self.assertIs(observed[0][1][1], s3_download_task)
        self.assertIs(observed[0][1][2], backend._s3_client)
        self.assertEqual(
            observed[0][1][3:5],
            ("media-bucket", "local_content/aa/bb/exact-key"),
        )

    @defer.inlineCallbacks
    def test_fetch_forwards_background_failure(self):
        background = defer.Deferred()

        def run_in_background(function, *args):
            return background

        backend = self._backend(SimpleNamespace(defer_to_threadpool=Mock()))
        with patch("s3_storage_provider.run_in_background", run_in_background):
            fetch = defer.ensureDeferred(
                backend.fetch(
                    "local_content/aa/bb/exact-key",
                    SimpleNamespace(),
                )
            )
            background.errback(RuntimeError("worker failed"))

            with self.assertRaises(RuntimeError):
                yield fetch


class S3DownloadTaskTestCase(unittest.TestCase):
    """Check S3 download lookup and responder setup without a live reactor."""

    @staticmethod
    def _call_from_thread(callback, *args, **kwargs):
        callback(*args, **kwargs)

    def test_missing_object_resolves_none(self):
        client = Mock()

        for error_code in ("404", "NoSuchKey", "NotFound"):
            with self.subTest(error_code=error_code):
                client.reset_mock()
                client.get_object.side_effect = ClientError(
                    {"Error": {"Code": error_code}}, "GetObject"
                )
                deferred = defer.Deferred()

                with patch(
                    "s3_storage_provider.reactor.callFromThread",
                    side_effect=self._call_from_thread,
                ):
                    s3_download_task(
                        client,
                        "media-bucket",
                        "local_content/aa/bb/exact-key",
                        deferred,
                    )

                self.assertIsNone(self.successResultOf(deferred))
                client.get_object.assert_called_once_with(
                    Bucket="media-bucket", Key="local_content/aa/bb/exact-key"
                )

    def test_non_404_failure_errbacks_deferred(self):
        client = Mock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "GetObject"
        )
        deferred = defer.Deferred()

        with patch(
            "s3_storage_provider.reactor.callFromThread",
            side_effect=self._call_from_thread,
        ):
            s3_download_task(
                client,
                "media-bucket",
                "local_content/aa/bb/exact-key",
                deferred,
            )

        failure = self.failureResultOf(deferred, ClientError)
        self.assertEqual(failure.value.response["Error"]["Code"], "AccessDenied")
        client.get_object.assert_called_once_with(
            Bucket="media-bucket", Key="local_content/aa/bb/exact-key"
        )

    def test_ordinary_worker_failure_errbacks_deferred(self):
        client = Mock()
        client.get_object.side_effect = RuntimeError("endpoint unavailable")
        deferred = defer.Deferred()

        with patch(
            "s3_storage_provider.reactor.callFromThread",
            side_effect=self._call_from_thread,
        ):
            s3_download_task(
                client,
                "media-bucket",
                "local_content/aa/bb/exact-key",
                deferred,
            )

        failure = self.failureResultOf(deferred, RuntimeError)
        self.assertEqual(str(failure.value), "endpoint unavailable")

    def test_invalid_body_errbacks_before_responder_callback(self):
        client = Mock()
        client.get_object.return_value = {}
        deferred = defer.Deferred()

        with patch(
            "s3_storage_provider.reactor.callFromThread",
            side_effect=self._call_from_thread,
        ), patch("s3_storage_provider._stream_to_producer") as stream:
            s3_download_task(
                client,
                "media-bucket",
                "local_content/aa/bb/exact-key",
                deferred,
            )

        self.failureResultOf(deferred, KeyError)
        stream.assert_not_called()

    def test_invalid_body_closes_body_and_preserves_close_failure(self):
        client = Mock()
        body = Mock()
        body.read = None
        body.close.side_effect = RuntimeError("close failed")
        client.get_object.return_value = {"Body": body}
        deferred = defer.Deferred()

        with patch(
            "s3_storage_provider.reactor.callFromThread",
            side_effect=self._call_from_thread,
        ):
            s3_download_task(
                client,
                "media-bucket",
                "local_content/aa/bb/exact-key",
                deferred,
            )

        failure = self.failureResultOf(deferred, ValueError)
        self.assertEqual(str(failure.value), "S3 get_object response has an invalid Body")
        self.assertIsNotNone(failure.value.__cause__)
        self.assertEqual(str(failure.value.__cause__), "close failed")
        body.close.assert_called_once_with()

    def test_success_sets_up_responder_and_streams_body(self):
        client = Mock()
        body = Mock()
        client.get_object.return_value = {"Body": body}
        deferred = defer.Deferred()

        with patch(
            "s3_storage_provider.reactor.callFromThread",
            side_effect=self._call_from_thread,
        ), patch("s3_storage_provider._stream_to_producer") as stream:
            s3_download_task(
                client,
                "media-bucket",
                "local_content/aa/bb/exact-key",
                deferred,
            )

        responder = self.successResultOf(deferred)
        self.assertIsInstance(responder, _S3Responder)
        client.get_object.assert_called_once_with(
            Bucket="media-bucket", Key="local_content/aa/bb/exact-key"
        )
        stream.assert_called_once_with(
            reactor,
            responder,
            body,
            timeout=90.0,
        )


class S3ConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.config = {
            "bucket": "telecrypt-test",
            "endpoint_url": "https://sss.telecrypt.io",
            "region_name": "telecrypt",
            "access_key_id": "access",
            "secret_access_key": "secret",
        }

    def test_requires_the_exact_runtime_config(self):
        parsed = S3StorageProviderBackend.parse_config(self.config)
        self.assertNotIn("prefix", parsed)
        self.assertEqual(parsed["endpoint_url"], "https://sss.telecrypt.io")

    def test_rejects_legacy_or_unknown_config(self):
        legacy = dict(self.config, prefix="media/")
        with self.assertRaises(ValueError):
            S3StorageProviderBackend.parse_config(legacy)

    def test_rejects_noncanonical_endpoint(self):
        noncanonical = dict(self.config, endpoint_url="https://s3.example.invalid")
        with self.assertRaisesRegex(ValueError, "https://sss.telecrypt.io"):
            S3StorageProviderBackend.parse_config(noncanonical)

    def test_rejects_legacy_s3_endpoint_without_alias_or_fallback(self):
        legacy = dict(self.config, endpoint_url="https://s3.telecrypt.io")
        with self.assertRaisesRegex(ValueError, "https://sss.telecrypt.io"):
            S3StorageProviderBackend.parse_config(legacy)


class StreamingProducerTestCase(unittest.TestCase):
    def setUp(self):
        self.reactor = ThreadedMemoryReactorClock()

        self.body = Channel()
        self.consumer = Mock()
        self.written = ""

        def write(data):
            self.written += data

        self.consumer.write.side_effect = write

        self.producer_status = _ProducerStatus()
        self.producer = _S3Responder()
        self.thread = Thread(
            target=_stream_to_producer,
            args=(self.reactor, self.producer, self.body),
            kwargs={"status": self.producer_status, "timeout": 1.0},
        )
        self.thread.daemon = True
        self.thread.start()

    def tearDown(self):
        # Really ensure that we've stopped the thread
        self.producer.stopProducing()

    def test_simple_produce(self):
        deferred = self.producer.write_to_consumer(self.consumer)

        self.body.write("test")
        self.wait_for_thread()
        self.assertEqual("test", self.written)

        self.body.write(" string")
        self.wait_for_thread()
        self.assertEqual("test string", self.written)

        self.body.finish()
        self.wait_for_thread()

        self.assertTrue(deferred.called)
        self.assertEqual(deferred.result, None)

    def test_pause_produce(self):
        deferred = self.producer.write_to_consumer(self.consumer)

        self.body.write("test")
        self.wait_for_thread()
        self.assertEqual("test", self.written)

        # We pause producing, but the thread will currently be blocked waiting
        # to read data, so we wake it up by writing before asserting that
        # it actually pauses.
        self.producer.pauseProducing()
        self.body.write(" string")
        self.wait_for_thread()
        self.producer_status.wait_until_paused(10.0)
        self.assertEqual("test string", self.written)

        # If we write again we remain paused and nothing gets written
        self.body.write(" second")
        self.producer_status.wait_until_paused(10.0)
        self.assertEqual("test string", self.written)

        # If we call resumeProducing the buffered data gets read and written.
        self.producer.resumeProducing()
        self.wait_for_thread()
        self.assertEqual("test string second", self.written)

        # We can continue writing as normal now
        self.body.write(" third")
        self.wait_for_thread()
        self.assertEqual("test string second third", self.written)

        self.body.finish()
        self.wait_for_thread()

        self.assertTrue(deferred.called)
        self.assertEqual(deferred.result, None)

    def test_error(self):
        deferred = self.producer.write_to_consumer(self.consumer)

        self.body.write("test")
        self.wait_for_thread()
        self.assertEqual("test", self.written)

        excp = Exception("Test Exception")
        self.body.error(excp)
        self.wait_for_thread()

        self.failureResultOf(deferred, Exception)

    def test_close_error_fails_the_stream(self):
        deferred = self.producer.write_to_consumer(self.consumer)
        self.body.close = Mock(side_effect=Exception("close failed"))

        self.body.finish()
        self.wait_for_thread()

        failure = self.failureResultOf(deferred, Exception)
        self.assertEqual(str(failure.value), "close failed")

    def test_preserves_stream_and_body_close_failures(self):
        deferred = self.producer.write_to_consumer(self.consumer)
        self.body.close = Mock(side_effect=Exception("close failed"))

        self.body.error(Exception("stream failed"))
        self.wait_for_thread()

        failure = self.failureResultOf(deferred, Exception)
        self.assertEqual(str(failure.value), "stream failed")
        self.assertIsNotNone(failure.value.__cause__)
        self.assertEqual(str(failure.value.__cause__), "close failed")

    def test_falsey_body_is_closed(self):
        deferred = self.producer.write_to_consumer(self.consumer)
        self.body.close = Mock()

        self.body.finish()
        self.wait_for_thread()

        self.assertTrue(deferred.called)
        self.body.close.assert_called_once_with()

    def test_consumer_write_failure_stops_and_fails_after_cleanup(self):
        deferred = self.producer.write_to_consumer(self.consumer)
        self.consumer.write.side_effect = Exception("consumer failed")
        self.body.close = Mock()

        self.body.write("test")
        self.wait_for_thread()
        self.wait_for_thread()

        failure = self.failureResultOf(deferred, Exception)
        self.assertEqual(str(failure.value), "consumer failed")
        self.body.close.assert_called_once_with()

    def test_unregister_failure_is_reported(self):
        deferred = self.producer.write_to_consumer(self.consumer)
        self.consumer.unregisterProducer.side_effect = Exception("unregister failed")

        self.body.finish()
        self.wait_for_thread()

        failure = self.failureResultOf(deferred, Exception)
        self.assertEqual(str(failure.value), "unregister failed")

    def test_registration_failure_propagates_without_unregistering(self):
        self.consumer.registerProducer.side_effect = Exception("register failed")
        self.body.close = Mock(side_effect=Exception("close failed"))

        deferred = self.producer.write_to_consumer(self.consumer)

        self.assertTrue(deferred.called)
        self.wait_for_thread()

        failure = self.failureResultOf(deferred, Exception)
        self.assertEqual(str(failure.value), "register failed")
        self.assertIsNotNone(failure.value.__cause__)
        self.assertEqual(str(failure.value.__cause__), "close failed")
        self.body.close.assert_called_once_with()
        self.consumer.unregisterProducer.assert_not_called()

    def test_cancellation_waits_for_close_and_preserves_close_failure(self):
        deferred = self.producer.write_to_consumer(self.consumer)
        self.body.close = Mock(side_effect=Exception("close failed"))

        self.producer.stopProducing()
        self.wait_for_thread()

        failure = self.failureResultOf(deferred, Exception)
        self.assertEqual(str(failure.value), "Consumer ask to stop producing")
        self.assertIsNotNone(failure.value.__cause__)
        self.assertEqual(str(failure.value.__cause__), "close failed")

    def wait_for_thread(self):
        """Wait for something to call `callFromThread` and advance reactor
        """
        self.reactor.thread_event.wait(1)
        self.reactor.thread_event.clear()
        self.reactor.advance(0)


class ThreadedMemoryReactorClock(MemoryReactorClock):
    """
    A MemoryReactorClock that supports callFromThread.
    """

    def __init__(self):
        super(ThreadedMemoryReactorClock, self).__init__()
        self.thread_event = Event()

    def callFromThread(self, callback, *args, **kwargs):
        """
        Make the callback fire in the next reactor iteration.
        """
        d = defer.Deferred()
        d.addCallback(lambda x: callback(*args, **kwargs))
        self.callLater(0, d.callback, True)

        self.thread_event.set()

        return d


class Channel(object):
    """Simple channel to mimic a thread safe file like object
    """

    def __init__(self):
        self._queue = Queue()

    def read(self, _):
        val = self._queue.get()
        if isinstance(val, Exception):
            raise val
        return val

    def write(self, val):
        self._queue.put(val)

    def error(self, err):
        self._queue.put(err)

    def finish(self):
        self._queue.put(None)

    def close(self):
        pass

    def __bool__(self):
        return False
