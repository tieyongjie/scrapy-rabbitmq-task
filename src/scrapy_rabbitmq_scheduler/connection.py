# -*- coding: utf-8 -*-

try:
    import pika
except ImportError:
    raise ImportError("Please install pika before running scrapy-rabbitmq.")


RABBITMQ_CONNECTION_TYPE = 'blocking'
RABBITMQ_QUEUE_NAME = 'scrapy_queue'
RABBITMQ_CONNECTION_PARAMETERS = {'host': 'localhost'}


def from_settings(settings):
    """ Factory method that returns an instance of channel
        :param str connection_type: This field can be `blocking`
            `asyncore`, `libev`, `select`, `tornado`, or `twisted`
        See pika documentation for more details:
            TODO: put pika url regarding connection type
        Parameters is a dictionary that can
        include the following values:
            :param str host: Hostname or IP Address to connect to
            :param int port: TCP port to connect to
            :param str virtual_host: RabbitMQ virtual host to use
            :param pika.credentials.Credentials credentials: auth credentials
            :param int channel_max: Maximum number of channels to allow
            :param int frame_max: The maximum byte size for an AMQP frame
            :param int heartbeat_interval: How often to send heartbeats
            :param bool ssl: Enable SSL
            :param dict ssl_options: Arguments passed to ssl.wrap_socket as
            :param int connection_attempts: Maximum number of retry attempts
            :param int|float retry_delay: Time to wait in seconds, before the next
            :param int|float socket_timeout: Use for high latency networks
            :param str locale: Set the locale value
            :param bool backpressure_detection: Toggle backpressure detection
        :return: Channel object
    """

    connection_type = settings.get('RABBITMQ_CONNECTION_TYPE', RABBITMQ_CONNECTION_TYPE)
    queue_name = settings.get('RABBITMQ_QUEUE_NAME', RABBITMQ_QUEUE_NAME)
    connection_parameters = settings.get('RABBITMQ_CONNECTION_PARAMETERS', RABBITMQ_CONNECTION_PARAMETERS)

    connection = {
        'blocking': pika.BlockingConnection,
        # 'libev': pika.LibevConnection,
        'select': pika.SelectConnection,
        # 'tornado': pika.TornadoConnection,
        # 'twisted': pika.TwistedConnection
    }[connection_type](pika.URLParameters(connection_parameters))

    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    return channel
        
def get_channel(connection, queue_name, durable=True, confirm_delivery=True, is_delay=False):
    """ Init method to return a prepared channel for consuming
    """
    channel = connection.channel()
    channel.queue_declare(queue=queue_name,durable=durable)#,arguments={'x-max-priority': 255,'vhost':'/'})
    if confirm_delivery:
        channel.confirm_delivery()

    if is_delay is True:
        exchange_name = "{}-delay".format(queue_name)
        channel.exchange_declare(exchange_name,)
                                #  exchange_type="x-delayed-message",
                                #  arguments={"x-delayed-type": "direct"})
        channel.queue_bind(
            queue=queue_name, exchange=exchange_name, routing_key=queue_name)
    return channel


def connect(connection_url):
    """ Create and return a fresh connection
    """
    return pika.BlockingConnection(pika.URLParameters(connection_url))