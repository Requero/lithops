#
# Module providing various facilities to other parts of the package
#
# multiprocessing/util.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Cloudlab URV
#
import traceback
import socket
import weakref
import redis
import pymemcache
import pika
import cloudpickle
import uuid
import logging
import lithops
import sys
import threading
import io
import itertools
from lithops.config import load_config

from . import config as mp_config

logger = logging.getLogger(__name__)


#
# Picklable redis client
#

class PicklableRedis(redis.StrictRedis):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        logger.debug('Creating picklable Redis client')
        self._type = 'redis'
        super().__init__(*self._args, **self._kwargs)

    def __getstate__(self):
        return self._args, self._kwargs

    def __setstate__(self, state):
        self.__init__(*state[0], **state[1])

    def get_type(self):
        return self._type

class PicklableMemcached(pymemcache.Client):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._type = 'memcached'
        super().__init__(*self._args, **self._kwargs)

    def __getstate__(self):
        return self._args, self._kwargs

    def __setstate__(self, state):
        self.__init__(*state[0], **state[1])

    def get_type(self):
        return self._type
        
#def get_redis_client(**overwrites):
def get_cache_client(**overwrites):
    try:
        if 'redis' in mp_config.get_parameter(mp_config.CACHE) :
            conn_params = load_config()['redis']
        elif 'memcached' in mp_config.get_parameter(mp_config.CACHE):
            conn_params = load_config()['memcached']
    except KeyError:
        raise Exception('None cache section not found in you config')
    conn_params.update(overwrites)

    if 'redis' in mp_config.get_parameter(mp_config.CACHE) :
        return PicklableRedis(**conn_params)
    if 'memcached' in mp_config.get_parameter(mp_config.CACHE) :
        return PicklableMemcached((conn_params['host'],conn_params['port']))


def get_amqp_client(**overwrites):
    try:
        if 'rabbitmq' in mp_config.get_parameter(mp_config.AMQP) :
            conn_params = load_config()['rabbitmq']
    except KeyError:
        raise Exception('None cache section not found in you config')
    conn_params.update(overwrites)

    if 'rabbitmq' in mp_config.get_parameter(mp_config.AMQP) :
        return pika.URLParameters(conn_params['amqp_url'])
#
# Helper functions
#

def get_uuid(length=12):
    return uuid.uuid1().hex[:length]


def make_stateless_script(script):
    # Make stateless redis Lua script (redis.client.Script)
    # Just to ensure no redis client is cache'd and avoid 
    # creating another connection when unpickling this object.
    script.registered_client = None
    return script


def export_execution_details(futures, lithops_executor):
    if mp_config.get_parameter(mp_config.EXPORT_EXECUTION_DETAILS):
        try:
            path = os.path.realpath(mp_config.get_parameter(mp_config.EXPORT_EXECUTION_DETAILS))
            job_id = futures[0].job_id
            plots_file_name = '{}_{}'.format(lithops_executor.executor_id, job_id)
            lithops_executor.plot(fs=futures, dst=os.path.join(path, plots_file_name))

            stats = {fut.call_id: fut.stats for fut in futures}
            stats_file_name = '{}_{}_stats.json'.format(lithops_executor.executor_id, job_id)
            with open(os.path.join(path, stats_file_name), 'w') as stats_file:
                stats_json = json.dumps(stats, indent=4)
                stats_file.write(stats_json)
        except Exception as e:
            logger.error('Error while exporting execution results: {}\n{}'.format(e, traceback.format_exc()))


def get_network_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.connect(('<broadcast>', 0))
    return s.getsockname()[0]

 #
# Remote logging
#

def setup_log_streaming(executor):
    if mp_config.get_parameter(mp_config.STREAM_STDOUT):
        stream = executor.executor_id
        logger.debug('Log streaming enabled, stream name: {}'.format(stream))
        remote_logger = RemoteLoggingFeed(stream)
        remote_logger.start()
        return remote_logger, stream
    else:
        return None, None

if 'redis' in  mp_config.get_parameter(mp_config.CACHE):

    #
    # object for counting remote references (redis keys)
    # and garbage collect them automatically when nothing
    # is pointing at them
    #

    class RemoteReference:
        def __init__(self, referenced, managed=False, client=None):
            if isinstance(referenced, str):
                referenced = [referenced]
            if not isinstance(referenced, list):
                raise TypeError("referenced must be a key (str) or"
                                "a list of keys")
            self._referenced = referenced

            # reference counter key
            self._rck = '{}-{}'.format('ref', self._referenced[0])
            self._referenced.append(self._rck)
            self._client = client or get_cache_client()

            self._callback = None
            self.managed = managed

        @property
        def managed(self):
            return self._callback is None

        @managed.setter
        def managed(self, value):
            managed = value

            if self._callback is not None:
                self._callback.atexit = False
                self._callback.detach()

            if managed:
                self._callback = None
            else:
                self._callback = weakref.finalize(self, type(self)._finalize,
                                                  self._client, self._rck, self._referenced)

        def __getstate__(self):
            return (self._rck, self._referenced,
                    self._client, self.managed)

        def __setstate__(self, state):
            (self._rck, self._referenced,
             self._client) = state[:-1]
            self._callback = None
            self.managed = state[-1]
            self.incref()

        def incref(self):
            if not self.managed:
                pipeline = self._client.pipeline()
                pipeline.incr(self._rck, 1)
                pipeline.expire(self._rck, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                counter, _ = pipeline.execute()
                return int(counter)

        def decref(self):
            if not self.managed:
                pipeline = self._client.pipeline()
                pipeline.decr(self._rck, 1)
                pipeline.expire(self._rck, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                counter, _ = pipeline.execute()
                return int(counter)

        def refcount(self):
            count = self._client.get(self._rck)
            return 1 if count is None else int(count) + 1

        def collect(self):
            if len(self._referenced) > 0:
                self._client.delete(*self._referenced)
                self._referenced = []

        @staticmethod
        def _finalize(client, rck, referenced):
            count = int(client.decr(rck, 1))
            if count < 0 and len(referenced) > 0:
                client.delete(*referenced)

elif 'memcached' in  mp_config.get_parameter(mp_config.CACHE):

    #
    # object for counting remote references (redis keys)
    # and garbage collect them automatically when nothing
    # is pointing at them
    #

    class RemoteReference:
        def __init__(self, referenced, managed=False, client=None):
            if isinstance(referenced, str):
                referenced = [referenced]
            if not isinstance(referenced, list):
                raise TypeError("referenced must be a key (str) or"
                                "a list of keys")
            self._referenced = referenced

            # reference counter key
            self._rck = '{}-{}'.format('ref', self._referenced[0])
            self._referenced.append(self._rck)
            self._client = client or get_cache_client()
            self._client.set(self._rck, 0)
            self._callback = None
            self.managed = managed

        @property
        def managed(self):
            return self._callback is None

        @managed.setter
        def managed(self, value):
            managed = value

            if self._callback is not None:
                self._callback.atexit = False
                self._callback.detach()

            if managed:
                self._callback = None
            else:
                self._callback = weakref.finalize(self, type(self)._finalize,
                                                self._client, self._rck, self._referenced)

        def __getstate__(self):
            return (self._rck, self._referenced,
                    self._client, self.managed)

        def __setstate__(self, state):
            (self._rck, self._referenced,
            self._client) = state[:-1]
            self._callback = None
            self.managed = state[-1]
            self.incref()

        def incref(self):
            if not self.managed:
                return int(self._client.incr(self._rck, 1))

        def decref(self):
            if not self.managed:
                return int(self._client.decr(self._rck, 1))

        def refcount(self):
            count = int(self._client.get(self._rck))
            return 1 if count is None else int(count) + 1

        def collect(self):
            if len(self._referenced) > 0:
                for ref in self._referenced:
                    self._client.delete(ref)
                self._referenced = []

        @staticmethod
        def _finalize(client, rck, referenced):
            count = int(client.decr(rck, 1))
            if count < 0 and len(referenced) > 0:
                for ref in self._referenced:
                    self._client.delete(ref)
            pass


if 'redis' in  mp_config.get_parameter(mp_config.CACHE) and mp_config.get_parameter(mp_config.AMQP) == '':


    class RemoteLogIOBuffer:
        def __init__(self, stream):
            self._feeder_thread = threading
            self._buff = io.StringIO()
            self._client = get_cache_client()
            self._stream = stream
            self._offset = 0

        def write(self, log):
            self._buff.write(log)
            # self.flush()
            self._old_stdout.write(log)

        def flush(self):
            self._buff.seek(self._offset)
            log = self._buff.read()
            self._client.publish(self._stream, log)
            self._offset = self._buff.tell()
            # self._buff = io.StringIO()
            # FIXME flush() does not empty the buffer?
            self._buff.flush()

        def start(self):
            import sys
            self._old_stdout = sys.stdout
            sys.stdout = self
            logger.debug('Starting remote logging feed to stream %s', self._stream)

        def stop(self):
            import sys
            sys.stdout = self._old_stdout
            logger.debug('Stopping remote logging feed to stream %s', self._stream)


    class RemoteLoggingFeed:
        def __init__(self, stream):
            self._logger_thread = threading.Thread(target=self._logger_monitor, args=(stream,))
            self._stream = stream
            self._enabled = False

        def _logger_monitor(self, stream):
            logger.debug('Starting logger monitor thread for stream {}'.format(stream))
            cache_pubsub = get_cache_client().pubsub()
            cache_pubsub.subscribe(stream)

            while self._enabled:
                msg = cache_pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if msg is None:
                    continue
                if 'data' in msg:
                    sys.stdout.write(msg['data'].decode('utf-8'))

            logger.debug('Logger monitor thread for stream {} finished'.format(stream))

        def start(self):
            # self._logger_thread.daemon = True
            self._enabled = True
            self._logger_thread.start()

        def stop(self):
            self._enabled = False
            self._logger_thread.join(5)

elif 'rabbitmq' in  mp_config.get_parameter(mp_config.AMQP) :

    class RemoteLogIOBuffer:
        def __init__(self, stream):
            self._feeder_thread = threading
            self._buff = io.StringIO()
            self._parameters = get_amqp_client()
            self._stream = stream
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(exchange='exchange-'+self._stream, exchange_type='fanout')
            self._channel.queue_declare(queue=self._stream)
            self._channel.queue_bind(exchange='exchange-'+self._stream, queue=self._stream)
            self._offset = 0

        def write(self, log):
            self._buff.write(log)
            # self.flush()
            self._old_stdout.write(log)

        def flush(self):
            self._buff.seek(self._offset)
            log = self._buff.read()
            self._channel.basic_publish(exchange='exchange-'+self._stream,routing_key=self._stream,body=log)
            self._offset = self._buff.tell()
            # self._buff = io.StringIO()
            # FIXME flush() does not empty the buffer?
            self._buff.flush()

        def start(self):
            import sys
            self._old_stdout = sys.stdout
            sys.stdout = self
            logger.debug('Starting remote logging feed to stream %s', self._stream)

        def stop(self):
            import sys
            sys.stdout = self._old_stdout
            logger.debug('Stopping remote logging feed to stream %s', self._stream)


    class RemoteLoggingFeed:
        def __init__(self, stream):
            self._logger_thread = threading.Thread(target=self._logger_monitor, args=(stream,))
            self._parameters = get_amqp_client()
            self._stream = stream
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(exchange='exchange-'+self._stream, exchange_type='fanout')
            self._channel.queue_declare(queue=self._stream)
            self._channel.queue_bind(exchange='exchange-'+self._stream, queue=self._stream)
            self._enabled = False

        def _logger_monitor(self, stream):
            while self._enabled:
                method, properties, body  = self._channel.basic_get(queue=self._stream, auto_ack=False)
                msg = body 
                if msg is None:
                    continue
                else:
                    sys.stdout.write(msg.decode('utf-8'))

            logger.debug('Logger monitor thread for stream {} finished'.format(stream))

        def start(self):
            # self._logger_thread.daemon = True
            self._enabled = True
            self._logger_thread.start()

        def stop(self):
            self._enabled = False
            self._logger_thread.join(5)


_afterfork_registry = weakref.WeakValueDictionary()
_afterfork_counter = itertools.count()

def _run_after_forkers():
    items = list(_afterfork_registry.items())
    items.sort()
    for (index, ident, func), obj in items:
        try:
            func(obj)
        except Exception as e:
            info('after forker raised exception %s', e)

def register_after_fork(obj, func):
    _afterfork_registry[(next(_afterfork_counter), id(obj), func)] = obj

#
# Finalization using weakrefs
#

_finalizer_registry = {}
_finalizer_counter = itertools.count()