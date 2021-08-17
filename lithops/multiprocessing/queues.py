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

if 'redis' in  mp_config.get_parameter(mp_config.CACHE) and '' ==  mp_config.get_parameter(mp_config.AMQP):

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

elif 'memcached' in  mp_config.get_parameter(mp_config.CACHE) and '' ==  mp_config.get_parameter(mp_config.AMQP):
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

        def join(self):
            with self._cond:
                if self._unfinished_tasks.get_value() != 0:
                    self._cond.wait()

elif 'rabbitmq' in  mp_config.get_parameter(mp_config.AMQP):

    import time
    import pika
    #
    # Queue type using a pipe, buffer and thread
    #

    class Queue:
        _sentinel = object()
        Empty = Empty
        Full = Full

        def __init__(self, maxsize=0):
            self._opid = 'queue'+str(os.getpid())
            self._parameters = util.get_amqp_client()
            self._closed = False
            self._maxsize = maxsize
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
            if self._maxsize > 0:
                args = {"x-max-length":maxsize}
            else:
                args = {}
            self._channel.queue_declare(queue=self._opid,arguments =args)
            self._after_fork()

        def __getstate__(self):
            return (self._maxsize, self._parameters, self._opid)

        def __setstate__(self, state):
            (self._maxsize,self._parameters, self._opid) = state
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
            self._maxsize = self._maxsize
            self._parameters = self._parameters
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()


        def put(self, obj, block=True, timeout=None):
            if self._closed:
                raise ValueError(f"Queue {self!r} is closed")
                
            if self._notfull:
                obj = cloudpickle.dumps(obj)
                self._channel.basic_publish(routing_key=self._opid,body=obj)


        def get(self, block=True, timeout=None):
            
            if block and timeout is None:
                global res
                res = None
                def callback(ch, method, properties, body):
                    global res
                    ch.stop_consuming()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    res = body
                #self._channel.basic_qos(prefetch_count=1)
                self._channel.basic_consume(queue=self._opid, on_message_callback=callback)
                self._channel.start_consuming()
                return cloudpickle.loads(res)
            else:
                res = None
                if block:
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                method, properties, body  = self._channel.basic_get(queue=self._opid, auto_ack=False)
                res = body
                return cloudpickle.loads(res)

        def qsize(self):
            queue_state = self._channel.queue_declare(queue=self._opid, passive = True)
            return queue_state.method.message_count

        def empty(self):
            queue_state = self._channel.queue_declare(queue=self._opid, passive = True)
            return queue_state.method.message_count == 0

        def full(self):
            queue_state = self._channel.queue_declare(queue=self._opid, passive = True)
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

        def _poll(self, timeout):
            max_time = time.monotonic() + timeout
            while time.monotonic() < max_time:
                qsize = self.qsize()
                if qsize > 0:
                    return True
                else:
                    time.sleep(0.1)
            return False

    #
    # Simplified Queue type
    #

    class SimpleQueue:
        def __init__(self):
            self._opid = 'simplequeue'+str(os.getpid())
            self._parameters = util.get_amqp_client()
            self._closed = False
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._opid)
            self._after_fork()

        def __getstate__(self):
            return (self._parameters, self._opid)

        def __setstate__(self, state):
            (self._parameters, self._opid) = state
            self._after_fork()

        def _after_fork(self):
            logger.debug('Queue._after_fork()')
            self._closed = False
            self._close = None
            self._parameters = self._parameters
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()


        def put(self, obj, block=True, timeout=None):
            if self._closed:
                raise ValueError(f"Queue {self!r} is closed")
                
            obj = cloudpickle.dumps(obj)
            self._channel.basic_publish(exchange='',routing_key=self._opid,body=obj)


        def get(self, block=True, timeout=None):
            
            if block and timeout is None:
                global res
                res = None
                def callback(ch, method, properties, body):
                    global res
                    res = body
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    ch.stop_consuming()
                #self._channel.basic_qos(prefetch_count=1)
                self._channel.basic_consume(queue=self._opid, on_message_callback=callback)
                self._channel.start_consuming()
            else:
                res = None
                if block:
                    if not self._poll(timeout):
                        raise Empty
                elif not self._poll():
                    raise Empty
                method, properties, body  = self._channel.basic_get(queue=self._opid, auto_ack=False)
                res = body
            return cloudpickle.loads(res)

        def qsize(self):
            queue_state = self._channel.queue_declare(queue=self._opid, passive = True)
            return queue_state.method.message_count

        def empty(self):
            queue_state = self._channel.queue_declare(queue=self._opid, passive = True)
            return queue_state.method.message_count == 0

        def full(self):
            return False

        def get_nowait(self):
            return self.get()

        def put_nowait(self, obj):
            return self.put(obj)

        def close(self):
            self._closed = True
            try:
                self._connection.close()
            finally:
                self._close = None
        
        def _poll(self, timeout):
            max_time = time.monotonic() + timeout
            while time.monotonic() < max_time:
                qsize = self.qsize()
                if qsize > 0:
                    return True
                else:
                    time.sleep(0.1)
            return False


    #
    # A queue type which also supports join() and task_done() methods
    #

    class JoinableQueue(Queue):
        def __init__(self):
            super().__init__()
            self._channel.exchange_declare(exchange='exchange-condition-'+self._opid, exchange_type='fanout')
            self._channel.queue_declare(queue='condition-'+self._opid)
            self._channel.queue_bind(exchange='exchange-condition-'+self._opid, queue='condition-'+self._opid)

        def _afterfork(self):
            super()._afterfork()
            #self._channel.exchange_declare(exchange=self._opid+'exchangecondition', exchange_type='fanout')
            #self._channel.queue_declare(queue=self._opid+'condition')

        def __getstate__(self):
            return (self._maxsize, self._parameters, self._opid)

        def __setstate__(self, state):
            (self._maxsize, self._parameters, self._opid) = state
            self._after_fork()

        def task_done(self):
            #if not self._unfinished_tasks.acquire(False):
            #    raise ValueError('task_done() called too many times')
            queue_state = self._channel.queue_declare(queue=self._opid, passive = True)
            a = queue_state.method.message_count
            if a == 0:
                self._channel.basic_publish(exchange='exchange-condition-'+self._opid,routing_key='condition-'+self._opid,body='notify')

        def join(self):
            def callback(ch, method, properties, body):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self._channel.stop_consuming()
            queue_state = self._channel.queue_declare(queue=self._opid, passive = True)
            a =queue_state.method.message_count
            if a != 0:
                self._channel.basic_consume(queue='condition-'+self._opid, on_message_callback=callback)
                self._channel.start_consuming()
