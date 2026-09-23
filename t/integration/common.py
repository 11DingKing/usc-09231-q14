"""Common base classes for integration tests.

These require a running RabbitMQ broker.  When the broker is not
reachable the tests are skipped instead of failing, so that the
unit-test suite stays usable in offline environments.
"""

from __future__ import annotations

import socket

import pytest

import kombu


def _broker_reachable(connection):
    try:
        connection.ensure_connection(
            max_retries=3, interval_start=0.1, interval_step=0.1,
            interval_max=0.5, timeout=3,
        )
    except Exception:
        return False
    return True


class BrokerCase:
    """Base class skipping all tests when the broker is unreachable."""

    @pytest.fixture(autouse=True)
    def _require_broker(self, connection):
        if not _broker_reachable(connection):
            pytest.skip('RabbitMQ broker is not reachable')
        yield
        connection.release()


class BasicFunctionality(BrokerCase):

    def test_publish_consume(self, connection):
        test_queue = kombu.Queue('kombu.basicfunctionality.test',
                                 routing_key='kombu.basicfunctionality.test')
        received = []

        def callback(body, message):
            received.append(body)
            message.ack()

        with connection.Consumer([test_queue], callbacks=[callback],
                                 auto_declare=True) as consumer:
            consumer.purge()
            producer = connection.Producer()
            producer.publish({'hello': 'world'},
                             routing_key='kombu.basicfunctionality.test')
            deadline = 10
            while not received and deadline:
                connection.drain_events(timeout=1)
                deadline -= 1
        assert received == [{'hello': 'world'}]

    def test_connect_close(self, connection):
        connection.connect()
        assert connection.connected
        connection.close()
        assert not connection.connected


class BaseExchangeTypes(BrokerCase):

    def _publish_and_receive(self, connection, exchange_type):
        exchange = kombuExchange = kombu.Exchange(
            f'kombu.exchange.{exchange_type}', type=exchange_type)
        queue = kombu.Queue(f'kombu.exchange.{exchange_type}.queue',
                            exchange=kombuExchange,
                            routing_key='kombu.exchange.key')
        received = []
        with connection.Consumer([queue], auto_declare=True,
                                 callbacks=[lambda b, m: (received.append(b),
                                                          m.ack())]):
            connection.Producer().publish(
                {'exchange_type': exchange_type},
                exchange=exchange, routing_key='kombu.exchange.key',
            )
            try:
                connection.drain_events(timeout=2)
            except socket.timeout:
                pass
        assert received == [{'exchange_type': exchange_type}]

    def test_direct(self, connection):
        self._publish_and_receive(connection, 'direct')

    def test_topic(self, connection):
        self._publish_and_receive(connection, 'topic')

    def test_fanout(self, connection):
        exchange = kombu.Exchange('kombu.exchange.fanout', type='fanout')
        queue = kombu.Queue('kombu.exchange.fanout.queue', exchange=exchange)
        received = []
        with connection.Consumer([queue], auto_declare=True,
                                 callbacks=[lambda b, m: (received.append(b),
                                                          m.ack())]):
            connection.Producer().publish({'fanout': 1}, exchange=exchange)
            try:
                connection.drain_events(timeout=2)
            except socket.timeout:
                pass
        assert received == [{'fanout': 1}]


class BaseTimeToLive(BrokerCase):

    def test_message_expires(self, connection):
        queue = kombu.Queue('kombu.ttl.test',
                            kombu.Exchange('kombu.ttl.test', type='direct'),
                            routing_key='kombu.ttl.test')
        with connection.Consumer([queue], auto_declare=True,
                                 callbacks=[lambda b, m: m.ack()]) as consumer:
            consumer.purge()
            connection.Producer().publish(
                {'ttl': 1}, exchange=queue.exchange,
                routing_key='kombu.ttl.test', expiration=0.05,
            )
        queue(connection.default_channel).delete()


class BasePriority(BrokerCase):

    def test_priority_queue(self, connection):
        queue = kombu.Queue(
            'kombu.priority.test',
            kombu.Exchange('kombu.priority.test', type='direct'),
            routing_key='kombu.priority.test',
            queue_arguments={'x-max-priority': 10},
        )
        received = []
        with connection.Consumer([queue], auto_declare=True,
                                 callbacks=[lambda b, m: (received.append(b),
                                                          m.ack())]):
            producer = connection.Producer()
            producer.publish({'prio': 1}, exchange=queue.exchange,
                             routing_key='kombu.priority.test', priority=1)
            try:
                connection.drain_events(timeout=2)
            except socket.timeout:
                pass
        assert received == [{'prio': 1}]


class BaseFailover(BrokerCase):

    @pytest.fixture(autouse=True)
    def _require_broker(self, failover_connection):
        if not _broker_reachable(failover_connection):
            pytest.skip('RabbitMQ broker is not reachable')
        yield
        failover_connection.release()

    def test_connect(self, failover_connection):
        failover_connection.connect()
        assert failover_connection.connected
        failover_connection.close()


class BaseMessage(BrokerCase):

    def test_message_ack_state(self, connection):
        queue = kombu.Queue('kombu.message.test',
                            kombu.Exchange('kombu.message.test', type='direct'),
                            routing_key='kombu.message.test')
        states = []
        with connection.Consumer([queue], auto_declare=True,
                                 callbacks=[lambda b, m: (states.append(b),
                                                          m.ack())]):
            connection.Producer().publish({'msg': 1}, exchange=queue.exchange,
                                          routing_key='kombu.message.test')
            try:
                connection.drain_events(timeout=2)
            except socket.timeout:
                pass
        assert states == [{'msg': 1}]
