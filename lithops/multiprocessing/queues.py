#
# Module implementing queues
#
# multiprocessing/queues.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Cloudlab URV
#

__all__ = ['Queue', 'SimpleQueue', 'JoinableQueue']

import os
import cloudpickle
import logging
import pika

from queue import Empty, Full

from . import connection
from . import util
from . import synchronize
from . import config as mp_config

logger = logging.getLogger(__name__)

if 'redis' in  mp_config.get_parameter(mp_config.CACHE):
    #
    # Queue type using a pipe, buffer and thread
    #

    class Queue:
        _sentinel = object()
        Empty = Empty
        Full = Full

        def __init__(self, maxsize=0):
            self._reader, self._writer = connection.Pipe(duplex=False, conn_type=connection.REDIS_LIST_CONN)
            self._ref = util.RemoteReference(referenced=[self._reader._handle, self._reader._subhandle],
                                            client=self._reader._client)
            self._opid = os.getpid()
            self._maxsize = maxsize

            self._after_fork()

        def __getstate__(self):
            return (self._maxsize, self._reader,
                    self._writer, self._opid, self._ref)

        def __setstate__(self, state):
            (self._maxsize, self._reader,
            self._writer, self._opid, self._ref) = state
            self._after_fork()

        @property
        def _notfull(self):
            if self._maxsize > 0:
                return self.qsize() < self._maxsize
            else:
                return True

        def _after_fork(self):
            logger.debug('Queue._after_fork()')
            self._closed = False
            self._close = None
            self._send_bytes = self._writer.send_bytes
            self._recv_bytes = self._reader.recv_bytes
            self._poll = self._reader.poll

        def put(self, obj, block=True, timeout=None):
            if self._closed:
                raise ValueError(f"Queue {self!r} is closed")

            if self._notfull:
                obj = cloudpickle.dumps(obj)
                self._send_bytes(obj)

        def get(self, block=True, timeout=None):
            if block and timeout is None:
                res = self._recv_bytes()
            else:
                if block:
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                res = self._recv_bytes()
            return cloudpickle.loads(res)

        def qsize(self):
            return len(self._reader)

        def empty(self):
            return not self._poll()

        def full(self):
            if self._maxsize > 0:
                return self.qsize() < self._maxsize
            else:
                return False

        def get_nowait(self):
            return self.get(False)

        def put_nowait(self, obj):
            return self.put(obj, False)

        def close(self):
            self._closed = True
            try:
                self._reader.close()
            finally:
                close = self._close
                if close:
                    self._close = None
                    close()

        def join_thread(self):
            logger.debug('Queue.join_thread()')
            assert self._closed

        def cancel_join_thread(self):
            logger.debug('Queue.cancel_join_thread()')
            pass


    #
    # Simplified Queue type
    #

    class SimpleQueue:
        def __init__(self):
            self._reader, self._writer = connection.Pipe(duplex=False)
            self._closed = False
            self._ref = util.RemoteReference(referenced=[self._reader._handle, self._reader._subhandle],
                                            client=self._reader._client)
            self._poll = self._reader.poll

        def put(self, obj, block=True, timeout=None):
            assert not self._closed
            obj = cloudpickle.dumps(obj)
            self._writer.send_bytes(obj)

        def get(self, block=True, timeout=None):
            if block and timeout is None:
                res = self._reader.recv_bytes()
            else:
                if block:
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                res = self._reader.recv_bytes()

            return cloudpickle.loads(res)

        def qsize(self):
            return len(self._reader)

        def empty(self):
            return not self._poll()

        def full(self):
            return False

        def get_nowait(self):
            return self.get()

        def put_nowait(self, obj):
            return self.put(obj)

        def close(self):
            if not self._closed:
                self._reader.close()
                self._closed = True


    #
    # A queue type which also supports join() and task_done() methods
    #

    class JoinableQueue(Queue):
        def __init__(self):
            super().__init__()
            self._unfinished_tasks = synchronize.Semaphore(0)
            self._cond = synchronize.Condition()

        def __getstate__(self):
            return (self._maxsize, self._reader,
                    self._writer, self._opid, self._ref,
                    self._unfinished_tasks, self._cond)

        def __setstate__(self, state):
            (self._maxsize, self._reader,
            self._writer, self._opid, self._ref,
            self._unfinished_tasks, self._cond) = state
            self._after_fork()

        def put(self, obj, block=True, timeout=None):
            with self._cond:
                super().put(obj)
                self._unfinished_tasks.release()

        def task_done(self):
            with self._cond:

                if not self._unfinished_tasks.acquire(False):
                    raise ValueError('task_done() called too many times')
                
                if self._unfinished_tasks.get_value() == 0:
                    self._cond.notify_all()

        def join(self):
            with self._cond:
                if self._unfinished_tasks.get_value() != 0:
                    self._cond.wait()

elif 'memcached' in  mp_config.get_parameter(mp_config.CACHE):
    #
    # Queue type using a pipe, buffer and thread
    #

    class Queue:
        _sentinel = object()
        Empty = Empty
        Full = Full

        def __init__(self, maxsize=0):
            self._reader, self._writer = connection.Pipe(duplex=False, conn_type=connection.MEMCACHED_CONN)
            self._ref = util.RemoteReference(referenced=[self._reader._handle, self._reader._subhandle],
                                            client=self._reader._client)
            self._opid = os.getpid()
            self._maxsize = maxsize

            self._after_fork()

        def __getstate__(self):
            return (self._maxsize, self._reader,
                    self._writer, self._opid, self._ref)

        def __setstate__(self, state):
            (self._maxsize, self._reader,
            self._writer, self._opid, self._ref) = state
            self._after_fork()

        @property
        def _notfull(self):
            if self._maxsize > 0:
                return self.qsize() < self._maxsize
            else:
                return True

        def _after_fork(self):
            logger.debug('Queue._after_fork()')
            self._closed = False
            self._close = None
            self._send_bytes = self._writer.send
            self._recv_bytes = self._reader.recv
            self._poll = self._reader.poll

        def put(self, obj, block=True, timeout=None):
            if self._closed:
                raise ValueError(f"Queue {self!r} is closed")
                
            if self._notfull:
                obj = cloudpickle.dumps(obj)
                self._send_bytes(obj)

        def get(self, block=True, timeout=None):
            if block and timeout is None:
                res = self._recv_bytes()
            else:
                if block:
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                res = self._recv_bytes()
            return cloudpickle.loads(res)

        def qsize(self):
            return len(self._reader)

        def empty(self):
            return not self._poll()

        def full(self):
            if self._maxsize > 0:
                return self.qsize() < self._maxsize
            else:
                return False

        def get_nowait(self):
            return self.get(False)

        def put_nowait(self, obj):
            return self.put(obj, False)

        def close(self):
            self._closed = True
            try:
                self._reader.close()
            finally:
                close = self._close
                if close:
                    self._close = None
                    close()

        def join_thread(self):
            logger.debug('Queue.join_thread()')
            assert self._closed

        def cancel_join_thread(self):
            logger.debug('Queue.cancel_join_thread()')
            pass



    #
    # Simplified Queue type
    #

    class SimpleQueue:
        def __init__(self):
            self._reader, self._writer = connection.Pipe(duplex=False, conn_type=connection.MEMCACHED_CONN)
            self._closed = False
            self._ref = util.RemoteReference(referenced=[self._reader._handle, self._reader._subhandle],
                                            client=self._reader._client)
            self._poll = self._reader.poll

        def put(self, obj, block=True, timeout=None):
            assert not self._closed
            obj = cloudpickle.dumps(obj)
            self._writer.send(obj)

        def get(self, block=True, timeout=None):
            if block and timeout is None:
                res = self._reader.recv()
            else:
                if block:
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                res = self._reader.recv()
            return cloudpickle.loads(res)

        def qsize(self):
            return len(self._reader)

        def empty(self):
            return not self._poll()

        def full(self):
            return False

        def get_nowait(self):
            return self.get()

        def put_nowait(self, obj):
            return self.put(obj)

        def close(self):
            if not self._closed:
                self._reader.close()
                self._closed = True


    #
    # A queue type which also supports join() and task_done() methods
    #

    class JoinableQueue(Queue):
        def __init__(self):
            super().__init__()
            self._unfinished_tasks = synchronize.Semaphore(0)
            self._cond = synchronize.Condition()

        def __getstate__(self):
            return (self._maxsize, self._reader,
                    self._writer, self._opid, self._ref,
                    self._unfinished_tasks, self._cond)

        def __setstate__(self, state):
            (self._maxsize, self._reader,
            self._writer, self._opid, self._ref,
            self._unfinished_tasks, self._cond) = state
            self._after_fork()

        def put(self, obj, block=True, timeout=None):
            with self._cond:
                super().put(obj)
                self._unfinished_tasks.release()

        def task_done(self):
            with self._cond:

                if not self._unfinished_tasks.acquire(False):
                    raise ValueError('task_done() called too many times')
                if self._unfinished_tasks.get_value() == 0:
                    self._cond.notify_all()
                    print('notify')

        def join(self):
            with self._cond:
                if self._unfinished_tasks.get_value() != 0:
                    self._cond.wait()

elif 'rabbitmq' in  mp_config.get_parameter(mp_config.CACHE):
    #
    # Queue type using a pipe, buffer and thread
    #

    class Queue:
        _sentinel = object()
        Empty = Empty
        Full = Full

        def __init__(self, maxsize=0):
            self._opid = os.getpid()
            self._parameters = pika.ConnectionParameters('localhost')
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = connection.channel()
            if maxsize > 0:
                args = {"x-max-length":str(maxsize)}
            else:
                args = {}
            self._channel.exchange_declare(exchange='logs',exchange_type='fanout')
            self._channel.queue_declare(queue=self._opid,auto_delete=False,arguments =args)
            """ 
            self._reader, self._writer = connection.Pipe(duplex=False, conn_type=connection.MEMCACHED_CONN)
            self._ref = util.RemoteReference(referenced=[self._reader._handle, self._reader._subhandle],
                                            client=self._reader._client) 
            """
            self._maxsize = maxsize
            self._after_fork()

        def __getstate__(self):
            return (self._maxsize, self._parameters,self._connection,
                    self._channel, self._opid, self._ref)

        def __setstate__(self, state):
            (self._maxsize,self._parameters,self._connection,
            self._channel, self._opid, self._ref) = state
            self._after_fork()

        @property
        def _notfull(self):
            if self._maxsize > 0:
                return self.qsize() < self._maxsize
            else:
                return True

        def _after_fork(self):
            logger.debug('Queue._after_fork()')
            self._closed = False
            self._close = None
            self._parameters = self._parameters
            self._connection = self._connection
            self._channel = self._channel
            #self._send_bytes = self._writer.send#_bytes
            #self._recv_bytes = self._reader.recv#_bytes
            #self._poll = self._reader.poll

        def put(self, obj, block=True, timeout=None):
            if self._closed:
                raise ValueError(f"Queue {self!r} is closed")
                
            if self._notfull:
                obj = cloudpickle.dumps(obj)
                #self._send_bytes(obj)
                self._channel.basic_publish(exchange='',routing_key=self._opid,body=obj)
                #connection.close()

        def get(self, block=True, timeout=None):
            res = None
            if block:
                if timeout is None:

                    def callback(ch, method, properties, body):
                        #print(" [x] Received %r" % body.decode())
                        #time.sleep(body.count(b'.'))
                        #print(" [x] Done")
                        #ch.basic_ack(delivery_tag=method.delivery_tag)
                        res = body
                        ch.stop_consuming()

                    #print(channel.get_waiting_message_count())
                    channel.basic_qos(prefetch_count=1)
                    channel.basic_consume(queue=self._opid, on_message_callback=callback)
                    channel.start_consuming()
            else:
                method, properties, body  = self._channel.basic_get(queue=self._opid, auto_ack  = True)
                res = body
            return cloudpickle.loads(res)

        def qsize(self):
            queue_state = self._channel.queue_declare(queue=self._opid, durable=True, passive = True)
            return queue_state.method.message_count

        def empty(self):
            queue_state = self._channel.queue_declare(queue=self._opid, durable=True, passive = True)
            return queue_state.method.message_count == 0

        def full(self):
            queue_state = self._channel.queue_declare(queue=self._opid, durable=True, passive = True)
            return queue_state.method.message_count == self.maxsize

        def get_nowait(self):
            return self.get(False)

        def put_nowait(self, obj):
            return self.put(obj, False)

        def close(self):
            self._closed = True
            try:
                self._connection.close()
            finally:
                self._close = None

        def join_thread(self):
            logger.debug('Queue.join_thread()')
            assert self._closed

        def cancel_join_thread(self):
            logger.debug('Queue.cancel_join_thread()')
            pass



    #
    # Simplified Queue type
    #

    class SimpleQueue:
        def __init__(self):
            """ self._reader, self._writer = connection.Pipe(duplex=False, conn_type=connection.MEMCACHED_CONN)
            self._ref = util.RemoteReference(referenced=[self._reader._handle, self._reader._subhandle],
                                            client=self._reader._client)
            self._poll = self._reader.poll """
            self._opid = os.getpid()
            self._parameters = pika.ConnectionParameters('localhost')
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = connection.channel()
            self._channel.queue_declare(queue=self._opid,auto_delete=False)
            self._maxsize = maxsize
            self._after_fork()

        def put(self, obj, block=True, timeout=None):
            assert not self._closed

            obj = cloudpickle.dumps(obj)
            #self._send_bytes(obj)
            self._channel.basic_publish(exchange='',routing_key=self._opid,body=obj)
            connection.close()

        def get(self, block=True, timeout=None):
            res = None
            if block:
                if timeout is None:

                    def callback(ch, method, properties, body):
                        #print(" [x] Received %r" % body.decode())
                        #time.sleep(body.count(b'.'))
                        #print(" [x] Done")
                        #ch.basic_ack(delivery_tag=method.delivery_tag)
                        res = body
                        ch.stop_consuming()

                    #print(channel.get_waiting_message_count())
                    channel.basic_qos(prefetch_count=1)
                    channel.basic_consume(queue=self._opid, on_message_callback=callback)
                    channel.start_consuming()
            else:
                method, properties, body  = self._channel.basic_get(queue=self._opid, auto_ack  = True)
                res = body
            return cloudpickle.loads(res)

        def qsize(self):
            queue_state = self._channel.queue_declare(queue=self._opid, durable=True, passive = True)
            return queue_state.method.message_count == 0

        def empty(self):
            queue_state = self._channel.queue_declare(queue=self._opid, durable=True, passive = True)
            return queue_state.method.message_count == 0

        def full(self):
            return False

        def get_nowait(self):
            return self.get()

        def put_nowait(self, obj):
            return self.put(obj)

        def close(self):
            if not self._closed:
                self._connection.close()
                self._closed = True


    #
    # A queue type which also supports join() and task_done() methods
    #

    class JoinableQueue(Queue):
        def __init__(self):
            super().__init__()
            #self._unfinished_tasks = synchronize.Semaphore(0)
            #self._cond = synchronize.Condition()

        def __getstate__(self):
            return (self._maxsize, self._parameters,self._connection,
                    self._channel, self._opid, self._ref)

        def __setstate__(self, state):
            (self._maxsize, self._parameters,self._connection,
            self._channel, self._opid, self._ref) = state
            self._after_fork()

        def put(self, obj, block=True, timeout=None):
            print('put')
            with self._cond:
                super().put(obj)
                self._unfinished_tasks.release() 

        def task_done(self):
            with self._cond:

                if not self._unfinished_tasks.acquire(False):
                    raise ValueError('task_done() called too many times')
                
                if self._unfinished_tasks.get_value() == 0:
                    self._cond.notify_all()

        def join(self):
            print('join')
            with self._cond:
                if self._unfinished_tasks.get_value() != 0:
                    self._cond.wait()