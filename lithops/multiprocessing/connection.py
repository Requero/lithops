#
# A higher level module for using sockets (or Windows named pipes)
#
# multiprocessing/connection.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Cloudlab URV
#

import time
import socket
import selectors
import threading
import random
import io
import logging
import pika
import cloudpickle

from multiprocessing.context import BufferTooShort

from . import util
from . import config as mp_config
from queue import Queue

import pynng

logger = logging.getLogger(__name__)

#
# Constants
#

# Handle prefixes
# (Separated keys/channels so that a given connection cannot read its own messages)
REDIS_LIST_CONN = 'redislist'  # uses Redis lists
REDIS_LIST_CONN_A = REDIS_LIST_CONN + '-a-'
REDIS_LIST_CONN_B = REDIS_LIST_CONN + '-b-'

REDIS_PUBSUB_CONN = 'redispubsub'  # uses Redis channels (pub/sub)
REDIS_PUBSUB_CONN_A = REDIS_PUBSUB_CONN + '-a-'
REDIS_PUBSUB_CONN_B = REDIS_PUBSUB_CONN + '-b-'

NANOMSG_REQREP_CONN = 'nanomsgreqrep'  # uses TCP sockets (nanomessage)
NANOMSG_REQREP_CONN_A = NANOMSG_REQREP_CONN + '-a-'
NANOMSG_REQREP_CONN_B = NANOMSG_REQREP_CONN + '-b-'

NANOMSG_PUBSUB_CONN = 'nanomsgpubsub'  # uses TCP sockets (nanomessage)
NANOMSG_PUBSUB_CONN_A = NANOMSG_PUBSUB_CONN + '-a-'
NANOMSG_PUBSUB_CONN_B = NANOMSG_PUBSUB_CONN + '-b-'

MEMCACHED_CONN = 'memcached'  # uses TCP sockets (nanomessage)
MEMCACHED_CONN_A = MEMCACHED_CONN + '-a-'
MEMCACHED_CONN_B = MEMCACHED_CONN + '-b-'

RABBITMQ_REQREP_CONN = 'rabbitmqreqrep'  # uses TCP sockets (nanomessage)
RABBITMQ_REQREP_CONN_A = RABBITMQ_REQREP_CONN + '-a-'
RABBITMQ_REQREP_CONN_B = RABBITMQ_REQREP_CONN + '-b-'

RABBITMQ_PUBSUB_CONN = 'rabbitmqpubsub'  # uses TCP sockets (nanomessage)
RABBITMQ_PUBSUB_CONN_A = RABBITMQ_PUBSUB_CONN + '-a-'
RABBITMQ_PUBSUB_CONN_B = RABBITMQ_PUBSUB_CONN + '-b-'

MIN_PORT = 49152
MAX_PORT = 65536


#
#  Helper functions
#

def get_handle_pair(conn_type, from_id=None):
    if from_id is None:
        conn_id = util.get_uuid()
    else:
        conn_id = from_id
    if conn_type == REDIS_LIST_CONN:
        return REDIS_LIST_CONN_A + conn_id, REDIS_LIST_CONN_B + conn_id
    elif conn_type == REDIS_PUBSUB_CONN:
        return REDIS_PUBSUB_CONN_A + conn_id, REDIS_PUBSUB_CONN_B + conn_id
    elif conn_type == NANOMSG_REQREP_CONN:
        return NANOMSG_REQREP_CONN_A + conn_id, NANOMSG_REQREP_CONN_B + conn_id
    elif conn_type == MEMCACHED_CONN:
        return MEMCACHED_CONN_A + conn_id, MEMCACHED_CONN_B + conn_id
    elif conn_type == NANOMSG_PUBSUB_CONN:
        return NANOMSG_PUBSUB_CONN_A + conn_id, NANOMSG_PUBSUB_CONN_B + conn_id
    elif conn_type == RABBITMQ_PUBSUB_CONN:
        return RABBITMQ_PUBSUB_CONN_A + conn_id, RABBITMQ_PUBSUB_CONN_B + conn_id
    else:
        raise Exception('Unknown connection type {}'.format(conn_type))


def get_subhandle(handle):
    if handle.startswith(REDIS_LIST_CONN_A):
        return REDIS_LIST_CONN_B + handle[len(REDIS_LIST_CONN_A):]
    elif handle.startswith(REDIS_LIST_CONN_B):
        return REDIS_LIST_CONN_A + handle[len(REDIS_LIST_CONN_B):]
    elif handle.startswith(REDIS_PUBSUB_CONN_A):
        return REDIS_PUBSUB_CONN_B + handle[len(REDIS_PUBSUB_CONN_A):]
    elif handle.startswith(REDIS_PUBSUB_CONN_B):
        return REDIS_PUBSUB_CONN_A + handle[len(REDIS_PUBSUB_CONN_B):]
    elif handle.startswith(MEMCACHED_CONN_A):
        return MEMCACHED_CONN_B + handle[len(MEMCACHED_CONN_A):]
    elif handle.startswith(MEMCACHED_CONN_B):
        return MEMCACHED_CONN_A + handle[len(MEMCACHED_CONN_B):]
    elif handle.startswith(NANOMSG_REQREP_CONN_A):
        return NANOMSG_REQREP_CONN_B + handle[len(NANOMSG_REQREP_CONN_A):]
    elif handle.startswith(NANOMSG_REQREP_CONN_B):
        return NANOMSG_REQREP_CONN_A + handle[len(NANOMSG_REQREP_CONN_B):]
    elif handle.startswith(NANOMSG_PUBSUB_CONN_A):
        return NANOMSG_PUBSUB_CONN_B + handle[len(NANOMSG_PUBSUB_CONN_A):]
    elif handle.startswith(NANOMSG_PUBSUB_CONN_B):
        return NANOMSG_PUBSUB_CONN_A + handle[len(NANOMSG_PUBSUB_CONN_B):]
    elif handle.startswith(RABBITMQ_PUBSUB_CONN_A):
        return RABBITMQ_PUBSUB_CONN_B + handle[len(RABBITMQ_PUBSUB_CONN_A):]
    elif handle.startswith(RABBITMQ_PUBSUB_CONN_B):
        return RABBITMQ_PUBSUB_CONN_A + handle[len(RABBITMQ_PUBSUB_CONN_B):]
    raise ValueError("bad handle prefix '{}' - "
                     "see lithops.multiprocessing.connection handle prefixes".format(handle))


def _validate_address(address):
    if not isinstance(address, str):
        raise ValueError("address must be a str, got {}".format(type(address)))
    if not address.startswith((REDIS_LIST_CONN, REDIS_PUBSUB_CONN, NANOMSG_REQREP_CONN, MEMCACHED_CONN)):
        raise ValueError("address '{}' is not of any known type ({}, {})".format(address,
                                                                                 REDIS_LIST_CONN,
                                                                                 REDIS_PUBSUB_CONN,
                                                                                 NANOMSG_CONN, 
                                                                                 MEMCACHED_CONN))





def get_network_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.connect(('<broadcast>', 0))
    return s.getsockname()[0]


#
# Connection classes
#
class _ConnectionBase:
    _handle = None

    def __init__(self, handle, readable=True, writable=True):
        if not readable and not writable:
            raise ValueError("at least one of `readable` and `writable` must be True")
        self._handle = handle
        self._readable = readable
        self._writable = writable

    def __del__(self):
        if self._handle is not None:
            self._close()

    def _check_closed(self):
        if self._handle is None:
            raise OSError("handle is closed")

    def _check_readable(self):
        if not self._readable:
            raise OSError("connection is write-only")

    def _check_writable(self):
        if not self._writable:
            raise OSError("connection is read-only")

    def _bad_message_length(self):
        if self._writable:
            self._readable = False
        else:
            self.close()
        raise OSError("bad message length")

    @property
    def closed(self):
        """True if the connection is closed"""
        return self._handle is None

    @property
    def readable(self):
        """True if the connection is readable"""
        return self._readable

    @property
    def writable(self):
        """True if the connection is writable"""
        return self._writable

    def fileno(self):
        """File descriptor or handle of the connection"""
        self._check_closed()
        return self._handle

    def close(self):
        """Close the connection"""
        logger.debug('Closing connection')
        if self._handle is not None:
            try:
                self._close()
            finally:
                self._handle = None

    def _close(self):
        raise NotImplementedError()

    def send(self, obj):
        """Send a (picklable) object"""
        self._check_closed()
        self._check_writable()
        obj_bin = cloudpickle.dumps(obj)
        logger.debug('Connection send %i B', len(obj_bin))
        self._send_bytes(obj_bin)

    def send_bytes(self, buf, offset=0, size=None):
        """Send the bytes data from a bytes-like object"""
        self._check_closed()
        self._check_writable()
        m = memoryview(buf)
        # HACK for byte-indexing of non-bytewise buffers (e.g. array.array)
        if m.itemsize > 1:
            m = memoryview(bytes(m))
        n = len(m)
        if offset < 0:
            raise ValueError("offset is negative")
        if n < offset:
            raise ValueError("buffer length < offset")
        if size is None:
            size = n - offset
        elif size < 0:
            raise ValueError("size is negative")
        elif offset + size > n:
            raise ValueError("buffer length < offset + size")
        self._send_bytes(m[offset:offset + size])

    def _send_bytes(self, param):
        raise NotImplementedError()

    def recv_bytes(self, maxlength=None):
        """
        Receive bytes data as a bytes object.
        """
        self._check_closed()
        self._check_readable()
        if maxlength is not None and maxlength < 0:
            raise ValueError("negative maxlength")
        buf = self._recv_bytes(maxlength)
        if buf is None:
            self._bad_message_length()
        return buf

    def _recv_bytes(self, maxlength=None):
        raise NotImplementedError()

    def recv_bytes_into(self, buf, offset=0):
        """
        Receive bytes data into a writeable bytes-like object.
        Return the number of bytes read.
        """
        self._check_closed()
        self._check_readable()
        with memoryview(buf) as m:
            # Get bytesize of arbitrary buffer
            itemsize = m.itemsize
            bytesize = itemsize * len(m)
            if offset < 0:
                raise ValueError("negative offset")
            elif offset > bytesize:
                raise ValueError("offset too large")
            result = self._recv_bytes()
            result_buff = io.BytesIO()
            result_buff.write(result)
            size = result_buff.tell()
            if bytesize < offset + size:
                raise BufferTooShort(result_buff.getvalue())
            # Message can fit in dest
            result_buff.seek(0)
            result_buff.readinto(m[offset // itemsize: (offset + size) // itemsize])
            return size

    def recv(self):
        """Receive a (picklable) object"""
        self._check_closed()
        self._check_readable()
        buf = self._recv_bytes()
        #logger.debug('Connection received %i B', len(buf))
        return cloudpickle.loads(buf)

    def poll(self, timeout=0.0):
        """Whether there is any input available to be read"""
        # TODO fix poll (always returns True)
        self._check_closed()
        self._check_readable()
        return self._poll(timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()


if 'redis' in  mp_config.get_parameter(mp_config.CACHE) and util.mp_config.get_parameter(mp_config.AMQP) == '':

   
    class _RedisConnection(_ConnectionBase):
        """
        Connection class for Redis.
        """
        _write = None
        _read = None

        def __init__(self, handle, readable=True, writable=True):
            super().__init__(handle, readable, writable)
            logger.debug('Requested creation of Redis connection resource')
            #self._client = util.get_redis_client()
            self._client = util.get_cache_client()
            self._subhandle = get_subhandle(handle)
            self._connect()

        def _connect(self):
            if self._handle.startswith(REDIS_LIST_CONN):
                logger.debug('Reconstruct Redis list connection')
                self._read = self._listread
                self._write = self._listwrite
                self._pubsub = None
            elif self._handle.startswith(REDIS_PUBSUB_CONN):
                logger.debug('Reconstruct Redis pubsub connection')
                self._read = self._channelread
                self._write = self._channelwrite
                self._pubsub = self._client.pubsub()
                self._pubsub.subscribe(self._handle)
            else:
                raise Exception('Unknown connection type {}'.format(self._handle))

        def __getstate__(self):
            return (self._client, self._handle, self._subhandle,
                    self._readable, self._writable)

        def __setstate__(self, state):
            (self._client, self._handle, self._subhandle,
            self._readable, self._writable) = state
            self._connect()

        def __len__(self):
            return self._client.llen(self._handle)

        def _set_expiry(self, key):
            logger.debug('Set key %s expiry time', key)
            self._client.expire(key, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            self._set_expiry = lambda key: None

        def _close(self, _close=None):
            if self._pubsub is not None:
                self._pubsub.unsubscribe(self._handle)
            # older versions of StrictRedis can't be closed
            if hasattr(self._client, 'close'):
                self._client.close()

        def _listwrite(self, handle, buf):
            self._set_expiry(handle)
            return self._client.rpush(handle, buf)

        def _listread(self, handle):
            _, v = self._client.blpop([handle])
            return v

        def _channelwrite(self, handle, buf):
            return self._client.publish(handle, buf)

        def _channelread(self, handle):
            consume = True
            while consume:
                msg = self._pubsub.get_message(timeout=5)
                if msg is not None and 'type' in msg:
                    if msg['type'] == 'subscribe':
                        continue
                    if msg['type'] == 'message':
                        return msg['data']

        def _send(self, buf, write=None):
            raise NotImplementedError('Connection._send() on Redis')

        def _recv(self, size, read=None):
            raise NotImplementedError('Connection._recv() on Redis')

        def _send_bytes(self, buf):
            self._write(self._subhandle, buf)

        def _recv_bytes(self, maxsize=None):
            msg = self._read(self._handle)
            return msg

        def _poll(self, timeout):
            if self._pubsub:
                r = wait([(self._pubsub, self._handle)], timeout)
            else:
                r = wait([(self._client, self._handle)], timeout)
            return bool(r)

    class _NanomsgConnection(_ConnectionBase):
        """
        Connection class for PyNNG
        """
        _buff = Queue()
        def __init__(self, handle, readable=True, writable=True):
            logger.debug('Requested creation of Nanomsg connection resource')
            super().__init__(handle, readable, writable)
            #self._client = util.get_redis_client()
            self._client = util.get_cache_client()
            #self._buff = Queue()
            self._subhandle = get_subhandle(handle)
            self._connect()

        def _connect(self):
            
            self._rep = pynng.Rep0()

            bind = False
            while not bind:
                try:
                    addr = 'tcp://' + get_network_ip() + ':' + str(random.randrange(MIN_PORT, MAX_PORT))
                    self._rep.listen(addr)
                    logger.debug('Assigned server address is %s', addr)
                    bind = True
                except pynng.exceptions.AddressInUse:
                    pass

            self._listener = threading.Thread(target=self._listen)
            self._listener.daemon = True
            self._listener.start()

            self._req = pynng.Req0()

            logger.debug('Set server address %s as handle %s', addr, self._handle)
            self._client.set(self._handle, bytes(addr, 'utf-8'), ex=mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))

        def _listen(self):
            logger.debug('Server thread started')
            while True:
                try:
                    msg = self._rep.recv()
                    logger.debug('Message received of size %i B', len(msg))
                except pynng.exceptions.Closed:
                    break
                self._buff.put(msg)
                self._rep.send(b'ok')
            logger.debug('Server thread finished')

        def __getstate__(self):
            return (self._client, self._handle, self._subhandle,
                    self._readable, self._writable)

        def __setstate__(self, state):
            (self._client, self._handle, self._subhandle,
            self._readable, self._writable) = state
            self._connect()

        def __len__(self):
            return self._client.llen(self._handle)

        def __reduce__(self):
            self._close()
            return super().__reduce__()

        def _close(self, _close=None):
            self._rep.close()
            self._client.delete(self._handle)
            if self._req:
                self._req.close()
            if hasattr(self._client, 'close'):
                self._client.close()

        def _send(self, buf, write=None):
            raise NotImplementedError('Connection._send() on Redis')

        def _recv(self, size, read=None):
            raise NotImplementedError('Connection._recv() on Redis')

        def _send_bytes(self, buf):
            if self._req is None:
                self._req = pynng.Req0()

            logger.debug('Get address from directory for handle %s', self._subhandle)
            addr = self._client.get(self._subhandle)

            retry = 15
            retry_sleep = 1
            while addr is None:
                time.sleep(retry_sleep)
                retry_sleep += 0.5
                addr = self._client.get(self._subhandle)
                retry -= 1
                if retry == 0:
                    raise Exception('Server address could not be fetched for handle {}'.format(self._subhandle))

            addr = addr.decode('utf-8')
            logger.debug('Dialing %s', addr)
            self._req.dial(addr)
            logger.debug('Send %i B to %s', len(buf), addr)
            self._req.send(buf.obj)
            res = self._req.recv()
            logger.debug(res)

        def _recv_bytes(self, maxsize=None):
            chunk = self._buff.get()
            return chunk

        def _poll(self, timeout):
            max_time = time.monotonic() + timeout
            while time.monotonic() < max_time:
                qsize = self._buff.qsize()
                if qsize > 0:
                    return True
                else:
                    time.sleep(0.1)

    PipeConnection = _RedisConnection

    def wait(object_list, timeout=None):
        """
        Wait till an object in object_list is ready/readable.

        Returns list of those objects in object_list which are ready/readable.
        """
        if timeout is not None:
            deadline = time.monotonic() + timeout

        while True:
            ready = []
            for client, handle in object_list:
                if handle.startswith(REDIS_LIST_CONN):
                    l = client.llen(handle)
                    if l > 0:
                        ready.append((client, handle))
                elif handle.startswith(REDIS_PUBSUB_CONN) and client.connection.can_read():
                    ready.append((client, handle))

            if any(ready):
                return ready

            if timeout is not None:
                timeout = deadline - time.monotonic()
                if timeout < 0:
                    return ready
            time.sleep(0.1)

elif 'memcached' in  mp_config.get_parameter(mp_config.CACHE) and util.mp_config.get_parameter(mp_config.AMQP) == '':

    class _MemcachedConnection(_ConnectionBase):
        """
        Connection class for Memcached.
        """
        _write = None
        _read = None

        def __init__(self, handle, readable=True, writable=True):
            super().__init__(handle, readable, writable)
            logger.debug('Requested creation of Redis connection resource')
            #self._client = util.get_redis_client()
            self._mutex = 'mutex-' + handle.split('-')[2]
            self._client = util.get_cache_client()
            self._client.set(handle+'-len',0)
            self._client.set(handle+'-count',0)
            self._client.set(handle+'-first',0)
            self._subhandle = get_subhandle(handle)
            self._connect()

        def _connect(self):
            logger.debug('Reconstruct Redis list connection')
            self._read = self._listread
            self._write = self._listwrite
            self._pubsub = None

        def __getstate__(self):
            return (self._client, self._mutex, self._handle, self._subhandle,
                    self._readable, self._writable)

        def __setstate__(self, state):
            (self._client, self._mutex, self._handle, self._subhandle,
            self._readable, self._writable) = state
            self._connect()

        def __len__(self):
            return int(self._client.get(self._handle+'-len'))

        def _set_expiry(self, key):
            logger.debug('Set key %s expiry time', key)
            self._set_expiry = lambda key: None

        def _close(self, _close=None):
            # older versions of StrictRedis can't be closed
            if hasattr(self._client, 'close'):
                self._client.close()

        def _acquire(self):
            self._client.set(self._mutex, self._mutex+'dummy')
            while not self._client.add(self._mutex, self._mutex+'dummy',noreply=False):
                pass
        
        def _release(self):
            self._client.delete(self._mutex)

        def _listwrite(self, handle, buf):
            i = int(self._client.incr(handle+'-count',1))
            self._client.set(handle+str(i-1), buf)
            j=self._client.incr(handle+'-len',1)
            self._release()
            return j
            #return self._client.rpush(handle, buf)

        def _listread(self, handle):
            temp = int(self._client.get(handle+'-len'))
            if temp == 0:
                self._acquire()
            self._client.decr(handle+'-len',1)
            i = self._client.incr(handle+'-first',1)
            v = self._client.get(handle+str(i-1))
            self._client.delete(handle+str(i-1))
            #_, v = self._client.blpop([handle])
            return v

        def _send(self, buf, write=None):
            raise NotImplementedError('Connection._send() on Redis')

        def _recv(self, size, read=None):
            raise NotImplementedError('Connection._recv() on Redis')

        def _send_bytes(self, buf):
            self._write(self._subhandle, buf)

        def _recv_bytes(self, maxsize=None):
            msg = self._read(self._handle)
            return msg

        def _poll(self, timeout):
            r = wait_memcached([(self._client, self._handle)], timeout)
            return bool(r)


    class _NanomsgConnection(_ConnectionBase):
        """
        Connection class for PyNNG
        """
        _buff = Queue()
        def __init__(self, handle, readable=True, writable=True):
            super().__init__(handle, readable, writable)
            logger.debug('Requested creation of Nanomsg connection resource')
            self._client = util.get_cache_client()
            self._subhandle = get_subhandle(handle)
            self._connect()

        def _connect(self):
            if self._handle.startswith(NANOMSG_REQREP_CONN):
                self._rep = pynng.Rep0()
                bind = False
                while not bind:
                    try:
                        addr = 'tcp://' + util.get_network_ip() + ':' + str(random.randrange(MIN_PORT, MAX_PORT))
                        self._rep.listen(addr)
                        
                        logger.debug('Assigned server address is %s', addr)
                        bind = True
                    except pynng.exceptions.AddressInUse:
                        pass
                self._listener = threading.Thread(target=self._listen)
                self._listener.daemon = True
                self._listener.start()

                self._req = None

                logger.debug('Set server address %s as handle %s', addr, self._handle)
                self._client.set(self._handle, bytes(addr, 'utf-8'))
                self._client.set(self._handle+'len', 0)
                self._pubsub = None
            else:
                self._sub = pynng.Sub0()
                bind = False
                while not bind:
                    try:
                        addr = 'tcp://' + get_network_ip() + ':' + str(random.randrange(MIN_PORT, MAX_PORT))
                        self._sub.subscribe("")
                        self._sub.dial(addr)
                        logger.debug('Assigned server address is %s', addr)
                        bind = True
                    except pynng.exceptions.AddressInUse:
                        pass
                self._listener = threading.Thread(target=self._listen)
                self._listener.daemon = True
                self._listener.start()

                self._pub = None

                logger.debug('Set server address %s as handle %s', addr, self._handle)
                self._client.set(self._handle, bytes(addr, 'utf-8'))
                self._client.set(self._handle+'len', 0)
                self._pubsub = True

        def _listen(self):
            logger.debug('Server thread started')
            if self._handle.startswith(NANOMSG_REQREP_CONN):
                while True:
                    try:
                        msg = self._rep.recv()
                        logger.debug('Message received of size %i B', len(msg))
                    except pynng.exceptions.Closed:
                        break
                    self._buff.put(msg)
                    self._client.incr(self._subhandle+'len',1)
                    self._rep.send(b'ok')
            else:
                while True:
                    try:
                        msg = self._sub.recv()
                        logger.debug('Message received of size %i B', len(msg))
                    except pynng.exceptions.Closed:
                        break
                    self._buff.put(msg)
                    self._client.incr(self._subhandle+'len',1)
                    self._sub.send(b'ok')
            logger.debug('Server thread finished')

        def __getstate__(self):
            return (self._client, self._handle, self._subhandle,
                    self._readable, self._writable)

        def __setstate__(self, state):
            (self._client, self._handle, self._subhandle,
            self._readable, self._writable) = state
            self._connect()

        def __len__(self):
            return self._buff.qsize()
            #return int(self._client.get(self._subhandle+'len'))

        def __reduce__(self):
            self._close()
            return super().__reduce__()

        def _close(self, _close=None):
            if self._handle.startswith(NANOMSG_REQREP_CONN):
                self._rep.close()
                self._client.delete(self._handle)
                if self._req != None:
                    self._req.close()
                if hasattr(self._client, 'close'):
                    self._client.close()
            else:
                self._sub.close()
                self._client.delete(self._handle)
                if self._pub:
                    self._pub.close()
                if hasattr(self._client, 'close'):
                    self._client.close()

        def _send(self, buf, write=None):
            raise NotImplementedError('Connection._send() on Redis')

        def _recv(self, size, read=None):
            raise NotImplementedError('Connection._recv() on Redis')

        def _send_bytes(self, buf):
            if self._handle.startswith(NANOMSG_REQREP_CONN):
                if self._req is None:
                    self._req = pynng.Req0()
                #self._req = self._req.new_context()

                logger.debug('Get address from directory for handle %s', self._subhandle)
                addr = self._client.get(self._subhandle)

                retry = 15
                retry_sleep = 1
                while addr is None:
                    time.sleep(retry_sleep)
                    retry_sleep += 0.5
                    addr = self._client.get(self._subhandle)
                    retry -= 1
                    if retry == 0:
                        raise Exception('Server address could not be fetched for handle {}'.format(self._subhandle))
                addr = addr.decode('utf-8')
                logger.debug('Dialing %s', addr)
                self._req.dial(addr)
                logger.debug('Send %i B to %s', len(buf), addr)
                self._req.send(buf)
                res = self._req.recv()
                logger.debug(res)
            else:
                if self._pub is None:
                    self._pub = pynng.Pub0()

                logger.debug('Get address from directory for handle %s', self._subhandle)
                addr = self._client.get(self._subhandle)

                retry = 15
                retry_sleep = 1
                while addr is None:
                    time.sleep(retry_sleep)
                    retry_sleep += 0.5
                    addr = self._client.get(self._subhandle)
                    retry -= 1
                    if retry == 0:
                        raise Exception('Server address could not be fetched for handle {}'.format(self._subhandle))
                addr = addr.decode('utf-8')
                logger.debug('Dialing %s', addr)
                self._pub.dial(addr)
                logger.debug('Send %i B to %s', len(buf), addr)
                self._pub.send(buf)
                #res = self._pub.recv()
                #print(res)
                #logger.debug(res)

        def _recv_bytes(self, maxsize=None):
            chunk = self._buff.get()
            self._client.decr(self._subhandle+'len',1)
            return chunk

        def _poll(self, timeout):
            max_time = time.monotonic() + timeout
            while time.monotonic() < max_time:
                qsize = self._buff.qsize()
                if qsize > 0:
                    return True
                else:
                    time.sleep(0.1)
            return False
    

    class _ZeromqConnection(_ConnectionBase):
        """
        Connection class for PyNNG
        """
        _buff = Queue()
        def __init__(self, handle, readable=True, writable=True):
            super().__init__(handle, readable, writable)
            logger.debug('Requested creation of ZeroMQ connection resource')
            self._client = util.get_cache_client()
            self._subhandle = get_subhandle(handle)
            self._connect()

        def _connect(self):
            self._rep = pynng.Rep0()
            bind = False
            while not bind:
                try:
                    addr = 'tcp://' + get_network_ip() + ':' + str(random.randrange(MIN_PORT, MAX_PORT))
                    self._rep.listen(addr)
                    
                    logger.debug('Assigned server address is %s', addr)
                    bind = True
                except pynng.exceptions.AddressInUse:
                    pass
            self._listener = threading.Thread(target=self._listen)
            self._listener.daemon = True
            self._listener.start()

            self._req = None

            logger.debug('Set server address %s as handle %s', addr, self._handle)
            self._client.set(self._handle, bytes(addr, 'utf-8'))
            self._client.set(self._handle+'len', 0)
            self._pubsub = None

        def _listen(self):
            logger.debug('Server thread started')
            #self._ctx = self._rep.new_context()
            if self._handle.startswith(NANOMSG_REQREP_CONN):
                while True:
                    try:
                        msg = self._rep.recv()
                        #msg = self._ctx.recv()
                        logger.debug('Message received of size %i B', len(msg))
                    except pynng.exceptions.Closed:
                        break
                    self._buff.put(msg)
                    self._client.incr(self._subhandle+'len',1)
                    self._rep.send(b'ok')
            else:
                while True:
                    try:
                        msg = self._sub.recv()
                        logger.debug('Message received of size %i B', len(msg))
                    except pynng.exceptions.Closed:
                        break
                    self._buff.put(msg)
                    self._client.incr(self._subhandle+'len',1)
                    self._sub.send(b'ok')
            logger.debug('Server thread finished')

        def __getstate__(self):
            return (self._client, self._handle, self._subhandle,
                    self._readable, self._writable)

        def __setstate__(self, state):
            (self._client, self._handle, self._subhandle,
            self._readable, self._writable) = state
            self._connect()

        def __len__(self):
            return self._buff.qsize()
            #return int(self._client.get(self._subhandle+'len'))

        def __reduce__(self):
            self._close()
            return super().__reduce__()

        def _close(self, _close=None):
            if self._handle.startswith(NANOMSG_REQREP_CONN):
                self._rep.close()
                #self._ctx.close()
                self._client.delete(self._handle)
                if self._req:
                    self._req.close()
                if hasattr(self._client, 'close'):
                    self._client.close()
            else:
                self._sub.close()
                self._client.delete(self._handle)
                if self._pub:
                    self._pub.close()
                if hasattr(self._client, 'close'):
                    self._client.close()

        def _send(self, buf, write=None):
            raise NotImplementedError('Connection._send() on Redis')

        def _recv(self, size, read=None):
            raise NotImplementedError('Connection._recv() on Redis')

        def _send_bytes(self, buf):
            if self._handle.startswith(NANOMSG_REQREP_CONN):
                if self._req is None:
                    self._req = pynng.Req0()
                #self._req = self._req.new_context()

                logger.debug('Get address from directory for handle %s', self._subhandle)
                addr = self._client.get(self._subhandle)

                retry = 15
                retry_sleep = 1
                while addr is None:
                    time.sleep(retry_sleep)
                    retry_sleep += 0.5
                    addr = self._client.get(self._subhandle)
                    retry -= 1
                    if retry == 0:
                        raise Exception('Server address could not be fetched for handle {}'.format(self._subhandle))
                addr = addr.decode('utf-8')
                logger.debug('Dialing %s', addr)
                self._req.dial(addr)
                logger.debug('Send %i B to %s', len(buf), addr)
                self._req.send(buf)
                res = self._req.recv()
                logger.debug(res)
            else:
                if self._pub is None:
                    self._pub = pynng.Pub0()

                logger.debug('Get address from directory for handle %s', self._subhandle)
                addr = self._client.get(self._subhandle)

                retry = 15
                retry_sleep = 1
                while addr is None:
                    time.sleep(retry_sleep)
                    retry_sleep += 0.5
                    addr = self._client.get(self._subhandle)
                    retry -= 1
                    if retry == 0:
                        raise Exception('Server address could not be fetched for handle {}'.format(self._subhandle))
                addr = addr.decode('utf-8')
                logger.debug('Dialing %s', addr)
                self._pub.dial(addr)
                logger.debug('Send %i B to %s', len(buf), addr)
                self._pub.send(buf)
                #res = self._pub.recv()
                #logger.debug(res)

        def _recv_bytes(self, maxsize=None):
            chunk = self._buff.get()
            #self._client.set(self._subhandle+'list', cloudpickle.dumps(self._buff_list))
            #chunk = self._buff_queue.get()
            #self._buff_list.pop()
            
            self._client.decr(self._subhandle+'len',1)
            return chunk

        def _poll(self, timeout):
            max_time = time.monotonic() + timeout
            while time.monotonic() < max_time:
                qsize = self._buff.qsize()
                if qsize > 0:
                    return True
                else:
                    time.sleep(0.1)
            return False
    
    PipeConnection = _NanomsgConnection
    #PipeConnection = _MemcachedConnection
    def wait_memcached(object_list, timeout=None):
        """
        Wait till an object in object_list is ready/readable.

        Returns list of those objects in object_list which are ready/readable.
        """
        if timeout is not None:
            deadline = time.monotonic() + timeout

        while True:
            ready = []
            for client, handle in object_list:
                l = int(client.get(handle+'-len'))
                if l > 0:
                    ready.append((client, handle))

            if any(ready):
                return ready

            if timeout is not None:
                timeout = deadline - time.monotonic()
                if timeout < 0:
                    return ready
            time.sleep(0.1)

elif 'rabbitmq' in  mp_config.get_parameter(mp_config.AMQP) :

    class _RabbitmqConnection(_ConnectionBase):
        """
        Connection class for Redis.
        """
        _write = None
        _read = None

        def __init__(self, handle, readable=True, writable=True):
            super().__init__(handle, readable, writable)
            logger.debug('Requested creation of Redis connection resource')
            self._parameters = util.get_amqp_client()
            self._handle = handle
            self._subhandle = get_subhandle(handle)
            self._connect()

        def _connect(self):
            if self._handle.startswith(RABBITMQ_REQREP_CONN):
                logger.debug('Reconstruct RabbitMQ reqrep connection')
                self._read = self._listread
                self._write = self._listwrite
                self._pubsub = False
            elif self._handle.startswith(RABBITMQ_PUBSUB_CONN):
                logger.debug('Reconstruct RabbitMQ pubsub connection')
                self._parameters = self._parameters
                self._connection = pika.BlockingConnection(self._parameters)
                self._channel = self._connection.channel()
                self._channel.exchange_declare(exchange='exchange-'+self._subhandle, exchange_type='fanout')
                self._channel.queue_declare(queue=self._subhandle)
                self._channel.queue_bind(exchange='exchange-'+self._subhandle, queue=self._subhandle)
                self._read = self._channelread
                self._write = self._channelwrite
                self._pubsub = True
            else:
                raise Exception('Unknown connection type {}'.format(self._handle))

        def __getstate__(self):
            return (self._parameters, self._handle, self._subhandle,
                    self._readable, self._writable)

        def __setstate__(self, state):
            (self._parameters, self._handle, self._subhandle,
            self._readable, self._writable) = state
            self._connect()

        def __len__(self):
            queue_state = self._channel.queue_declare(queue=self._subhandle, passive = True)
            return queue_state.method.message_count

        def _set_expiry(self, key):
            logger.debug('Set key %s expiry time', key)
            #self._client.expire(key, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            #self._set_expiry = lambda key: None

        def _close(self, _close=None):
            if hasattr(self._connection, 'close'):
                self._connection.close()

        def _channelwrite(self, handle, buf):
            return self._channel.basic_publish(exchange='exchange-'+handle,routing_key=handle,body=buf)

        def _channelread(self, handle):
            global res
            res = None
            def callback(ch, method, properties, body):
                global res
                res = body
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.stop_consuming()
            #self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(queue=handle, on_message_callback=callback)
            self._channel.start_consuming()
            return res

        def _send(self, buf, write=None):
            raise NotImplementedError('Connection._send() on Redis')

        def _recv(self, size, read=None):
            raise NotImplementedError('Connection._recv() on Redis')

        def _send_bytes(self, buf):
            self._write(self._subhandle, buf)

        def _recv_bytes(self, maxsize=None):
            msg = self._read(self._handle)
            return msg

        def _poll(self, timeout):
            if self._pubsub:
                r = wait([(self._pubsub, self._handle)], timeout)
            else:
                r = wait([(self._client, self._handle)], timeout)
            return bool(r)

#
# Public functions
#

class Listener(object):
    """
    Returns a listener object.
    """

    def __init__(self, address=None, family=None, backlog=1, authkey=None):
        conn_type = mp_config.get_parameter(mp_config.PIPE_CONNECTION_TYPE)
        if conn_type == REDIS_LIST_CONN:
            self._listener = _RedisListener(address, family, backlog)
        else:
            raise Exception('Unknown connection type {}'.format(conn_type))

        if authkey is not None and not isinstance(authkey, bytes):
            raise TypeError('authkey should be a byte string')
        self._authkey = authkey

    def accept(self):
        """
        Accept a connection on the bound socket or named pipe of `self`.

        Returns a `Connection` object.
        """
        if self._listener is None:
            raise OSError('listener is closed')
        c = self._listener.accept()
        return c

    def close(self):
        """
        Close the bound socket or named pipe of `self`.
        """
        logger.debug('Closing listener connection with address %s', self.address)
        listener = self._listener
        if listener is not None:
            self._listener = None
            listener.close()

    address = property(lambda self: self._listener._address)
    last_accepted = property(lambda self: self._listener._last_accepted)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()


def Client(address, family=None, authkey=None):
    """
    Returns a Client instance
    """
    conn_type = mp_config.get_parameter(mp_config.PIPE_CONNECTION_TYPE)
    if conn_type == REDIS_LIST_CONN:
        return _RedisClient(address)
    else:
        raise Exception('Unknown connection type {}'.format(conn_type))


def Pipe(duplex=True, conn_type=None):
    """
    Returns pair of connection objects at either end of a pipe
    """
    if conn_type is None:
        conn_type = mp_config.get_parameter(mp_config.PIPE_CONNECTION_TYPE)
    
    if conn_type == REDIS_LIST_CONN or conn_type == REDIS_PUBSUB_CONN:
        connection = _RedisConnection
    elif conn_type == NANOMSG_REQREP_CONN or conn_type == NANOMSG_PUBSUB_CONN:
        connection = _NanomsgConnection
    elif conn_type == MEMCACHED_CONN:
        connection = _MemcachedConnection
    elif conn_type == RABBITMQ_PUBSUB_CONN:
        connection = _RabbitmqConnection
    else:
        raise Exception('Unknown connection type {}'.format(conn_type))

    h1, h2 = get_handle_pair(conn_type=conn_type)

    if duplex:
        c1 = connection(h1)
        c2 = connection(h2)
    else:
        c1 = connection(h1, writable=False)
        c2 = connection(h2, readable=False)

    return c1, c2


#
# Definitions for connections based on sockets
#

class _RedisListener:
    def __init__(self, address, family=None, backlog=1):
        logger.debug('Requested creation of Redis listener for address %s', address)
        self._address = address
        #self._client = util.get_redis_client()
        self._client = util.get_cache_client()
        self._connect()

        self._last_accepted = None
        self._unlink = None

    def _connect(self):
        self._pubsub = self._client.pubsub()
        ip, port = self._address
        chan = '{}:{}'.format(ip, port)
        logger.debug('Subscribe to topic %s', chan)
        self._pubsub.subscribe(chan)
        self._gen = self._pubsub.listen()
        # ignore first message (subscribe message)
        next(self._gen)

    def __getstate__(self):
        return (self._address, self._family, self._client,
                self._last_accepted, self._unlink)

    def __setstate__(self, state):
        (self._address, self._family, self._client,
         self._last_accepted, self._unlink) = state
        self._connect()

    def accept(self):
        msg = next(self._gen)
        logger.debug('Received event: %s', msg)
        client_subhandle = msg['data'].decode('utf-8')
        c = _RedisConnection(client_subhandle)
        c.send('OK')
        self._last_accepted = client_subhandle
        return c

    def close(self):
        try:
            self._pubsub.close()
            self._pubsub = None
            self._gen = None
            if hasattr(self._client, 'close'):
                self._client.close()
                self._client = None
        finally:
            unlink = self._unlink
            if unlink is not None:
                self._unlink = None
                unlink()


def _RedisClient(address):
    """
    Return a connection object connected to the socket given by `address`
    """
    h1, h2 = get_handle_pair(conn_type=REDIS_LIST_CONN)
    c = _RedisConnection(h1)
    #redis_client = util.get_redis_client()
    redis_client = util.get_cache_client()
    ip, port = address
    chan = '{}:{}'.format(ip, port)
    redis_client.publish(chan, bytes(h2, 'utf-8'))
    ack = c.recv()
    assert ack == 'OK'
    return c

#
# Wait
#

# poll/select have the advantage of not requiring any extra file
# descriptor, contrarily to epoll/kqueue (also, they require a single
# syscall).
if hasattr(selectors, 'PollSelector'):
    _WaitSelector = selectors.PollSelector
else:
    _WaitSelector = selectors.SelectSelector




