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

import sys
from threading import Event, Thread
from types import SimpleNamespace

from botocore.exceptions import ClientError, EndpointConnectionError
from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial import unittest

is_py2 = sys.version[0] == "2"
if is_py2:
    from Queue import Queue
else:
    from queue import Queue

from mock import Mock

from s3_storage_provider import (
    S3StorageProviderBackend,
    _delete_object,
    _stream_to_producer,
    _S3Responder,
    _ProducerStatus,
)


class S3DeleteTestCase(unittest.TestCase):
    def _backend(self, module_api, client, prefix="", extra_args=None):
        backend = object.__new__(S3StorageProviderBackend)
        backend._module_api = module_api
        backend._s3_client = client
        backend._s3_pool = object()
        backend.bucket = "media-bucket"
        backend.prefix = prefix
        backend.extra_args = extra_args or {}
        return backend

    @defer.inlineCallbacks
    def test_delete_uses_exact_bucket_and_key_without_prefix(self):
        client = Mock()
        observed = []

        async def defer_to_threadpool(*args, **kwargs):
            observed.append((args, kwargs))
            return args[1](*args[2:], **kwargs)

        module_api = SimpleNamespace(defer_to_threadpool=defer_to_threadpool)
        backend = self._backend(module_api, client)

        yield defer.ensureDeferred(
            backend.delete("local_content/aa/bb/exact-key", SimpleNamespace())
        )

        self.assertEqual(len(observed), 1)
        call_args, call_kwargs = observed[0]
        self.assertIs(call_args[0], backend._s3_pool)
        self.assertIs(call_args[1], _delete_object)
        self.assertEqual(
            call_args[2:], (client, "media-bucket", "local_content/aa/bb/exact-key")
        )
        self.assertEqual(call_kwargs, {})
        client.delete_object.assert_called_once_with(
            Bucket="media-bucket", Key="local_content/aa/bb/exact-key"
        )

    @defer.inlineCallbacks
    def test_delete_uses_prefix_and_does_not_send_upload_sse_arguments(self):
        client = Mock()
        observed = []

        async def defer_to_threadpool(*args, **kwargs):
            observed.append((args, kwargs))
            return args[1](*args[2:], **kwargs)

        module_api = SimpleNamespace(defer_to_threadpool=defer_to_threadpool)
        backend = self._backend(
            module_api,
            client,
            prefix="media/",
            extra_args={
                "StorageClass": "STANDARD_IA",
                "SSECustomerKey": "customer-key",
                "SSECustomerAlgorithm": "AES256",
            },
        )

        yield defer.ensureDeferred(
            backend.delete("local_content/aa/bb/exact-key", SimpleNamespace())
        )

        self.assertEqual(len(observed), 1)
        call_args, call_kwargs = observed[0]
        self.assertIs(call_args[0], backend._s3_pool)
        self.assertIs(call_args[1], _delete_object)
        self.assertEqual(
            call_args[2:],
            (client, "media-bucket", "media/local_content/aa/bb/exact-key"),
        )
        self.assertEqual(call_kwargs, {})
        client.delete_object.assert_called_once_with(
            Bucket="media-bucket", Key="media/local_content/aa/bb/exact-key"
        )

    def test_delete_treats_all_missing_object_errors_as_success(self):
        client = Mock()

        for error_code in ("404", "NoSuchKey", "NotFound"):
            with self.subTest(error_code=error_code):
                client.reset_mock()
                client.delete_object.side_effect = ClientError(
                    {"Error": {"Code": error_code}}, "DeleteObject"
                )

                _delete_object(client, "media-bucket", "media/local/abc")

                client.delete_object.assert_called_once_with(
                    Bucket="media-bucket", Key="media/local/abc"
                )

    def test_delete_propagates_permission_auth_transport_and_other_errors(self):
        client = Mock()
        errors = (
            ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject"),
            ClientError({"Error": {"Code": "InvalidAccessKeyId"}}, "DeleteObject"),
            EndpointConnectionError(endpoint_url="https://s3.example.invalid"),
            RuntimeError("unexpected failure"),
        )

        for error in errors:
            with self.subTest(error=error):
                client.delete_object.side_effect = error
                with self.assertRaises(type(error)):
                    _delete_object(client, "media-bucket", "media/local/abc")


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
