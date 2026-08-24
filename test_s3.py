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

from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.test.proto_helpers import MemoryReactorClock
from twisted.trial import unittest

import sys

is_py2 = sys.version[0] == "2"
if is_py2:
    from Queue import Queue
else:
    from queue import Queue

from tempfile import NamedTemporaryFile
from threading import Event, Thread

from botocore.exceptions import ClientError
from mock import Mock

from s3_storage_provider import (
    _delete_object,
    _put_object_from_file,
    _stream_to_producer,
    _S3Responder,
    _ProducerStatus,
    S3StorageProviderBackend,
)


class S3ObjectOperationTestCase(unittest.TestCase):
    def test_store_uses_one_put_object_from_the_given_source(self):
        client = Mock()
        observed = {}

        def put_object(**kwargs):
            observed.update(kwargs)
            observed["body"] = kwargs["Body"].read()

        client.put_object.side_effect = put_object

        with NamedTemporaryFile() as source:
            source.write(b"temporary media")
            source.flush()
            _put_object_from_file(
                client,
                "media-bucket",
                "media/local/abc",
                source.name,
                {},
            )

        self.assertEqual(observed["Bucket"], "media-bucket")
        self.assertEqual(observed["Key"], "media/local/abc")
        self.assertEqual(observed["body"], b"temporary media")
        client.put_object.assert_called_once()
        client.upload_file.assert_not_called()

    def test_delete_uses_the_exact_key(self):
        client = Mock()

        _delete_object(client, "media-bucket", "media/local/abc")

        client.delete_object.assert_called_once_with(
            Bucket="media-bucket", Key="media/local/abc"
        )

    def test_delete_treats_absent_object_as_success(self):
        client = Mock()
        client.delete_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "DeleteObject"
        )

        _delete_object(client, "media-bucket", "media/local/abc")

    def test_delete_propagates_other_errors(self):
        client = Mock()
        client.delete_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "DeleteObject"
        )

        with self.assertRaises(ClientError):
            _delete_object(client, "media-bucket", "media/local/abc")


class S3ConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.config = {
            "bucket": "telecrypt-test",
            "endpoint_url": "https://s3.telecrypt.io",
            "region_name": "telecrypt",
            "access_key_id": "access",
            "secret_access_key": "secret",
        }

    def test_requires_the_exact_runtime_config(self):
        parsed = S3StorageProviderBackend.parse_config(self.config)
        self.assertNotIn("prefix", parsed)

    def test_rejects_legacy_or_unknown_config(self):
        legacy = dict(self.config, prefix="media/")
        with self.assertRaises(ValueError):
            S3StorageProviderBackend.parse_config(legacy)

    def test_rejects_noncanonical_endpoint(self):
        noncanonical = dict(self.config, endpoint_url="https://s3.example.invalid")
        with self.assertRaises(ValueError):
            S3StorageProviderBackend.parse_config(noncanonical)


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
