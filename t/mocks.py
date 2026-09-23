"""Mocks for testing."""

from __future__ import annotations

from itertools import count

from kombu.transport import base
from kombu.utils import json


class Message(base.Message):

    def __init__(self, *args, **kwargs):
        self.throw_decode_error = kwargs.pop('throw_decode_error', False)
        super().__init__(*args, **kwargs)

    def decode(self):
        if self.throw_decode_error:
            raise ValueError("can't decode message")
        return super().decode()


class Channel(base.StdChannel):
    open = True
    throw_decode_error = False
    _ids = count(1)

    def __init__(self, connection):
        self.connection = connection
        self.channel_id = next(self._ids)
        self.called = []
        self.deliveries = count(1)
        self.to_deliver = []
        self.events = {'basic_return': set()}

    def _receive_one(self, c):
        try:
            message = self.to_deliver.pop()
        except IndexError:
            pass
        else:
            return self.message_to_python(message)

    def basic_publish(self, message, exchange='', routing_key='',
                      mandatory=False, immediate=False, timeout=None,
                      confirm_timeout=None, **kwargs):
        self.called.append('basic_publish')
        message['delivery_tag'] = next(self.deliveries)
        return message, exchange, routing_key

    def exchange_declare(self, *args, **kwargs):
        self.called.append('exchange_declare')

    def queue_declare(self, *args, **kwargs):
        self.called.append('queue_declare')

    def queue_bind(self, *args, **kwargs):
        self.called.append('queue_bind')

    def queue_purge(self, *args, **kwargs):
        self.called.append('queue_purge')

    def basic_consume(self, *args, **kwargs):
        self.called.append('basic_consume')

    def basic_cancel(self, *args, **kwargs):
        self.called.append('basic_cancel')

    def basic_qos(self, *args, **kwargs):
        self.called.append('basic_qos')

    def basic_ack(self, *args, **kwargs):
        self.called.append('basic_ack')

    def basic_recover(self, *args, **kwargs):
        self.called.append('basic_recover')

    def basic_reject(self, tag, requeue=False):
        if requeue:
            return self.called.append('basic_reject:requeue')
        return self.called.append('basic_reject')

    def flow(self, active):
        self.called.append('flow')

    def prepare_message(self, body, priority, content_type,
                        content_encoding, headers, properties):
        self.called.append('prepare_message')
        return {'body': body,
                'headers': headers,
                'properties': properties,
                'priority': priority,
                'content_type': content_type,
                'content_encoding': content_encoding}

    def message_to_python(self, message, *args, **kwargs):
        self.called.append('message_to_python')
        return Message(
            body=json.dumps(message),
            delivery_tag=next(self.deliveries),
            content_type='application/json',
            content_encoding='utf-8',
            delivery_info={},
            properties={},
            headers={},
            throw_decode_error=self.throw_decode_error,
            channel=self,
        )

    def basic_get(self, *args, **kwargs):
        return self._receive_one(1)

    def close(self):
        self.called.append('close')
        self.open = False

    def __contains__(self, key):
        return key in self.called


class Connection:
    connected = True

    def __init__(self):
        self.channels = []
        self.client = None

    def channel(self):
        chan = Channel(self)
        self.channels.append(chan)
        return chan

    def close(self):
        self.connected = False


class Transport(base.Transport):
    Connection = Connection

    def establish_connection(self):
        conn = Connection()
        conn.client = self.client
        return conn

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return 'event'

    def close_connection(self, connection):
        connection.close()
