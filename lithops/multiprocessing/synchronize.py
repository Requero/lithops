#
# Module implementing synchronization primitives
#
# multiprocessing/synchronize.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Cloudlab URV
#

import threading
import time
import logging
import cloudpickle

from . import util
from . import config as mp_config
from . import queues
logger = logging.getLogger(__name__)

#
# Constants
#

SEM_VALUE_MAX = 2 ** 30

if util. mp_config.get_parameter(mp_config.CACHE) == 'redis': 
    #
    # Base class for semaphores and mutexes
    #

    class SemLock:
        # KEYS[1] - semlock name
        # ARGV[1] - max value
        # return new semlock value
        # only increments its value if
        # it is not above the max value
        LUA_RELEASE_SCRIPT = """
            local current_value = tonumber(redis.call('llen', KEYS[1]))
            if current_value >= tonumber(ARGV[1]) then
                return current_value
            end
            redis.call('rpush', KEYS[1], '')
            return current_value + 1
        """

        def __init__(self, value=1, max_value=1):
            self._name = 'semlock-' + util.get_uuid()
            self._max_value = max_value
            self._client = util.get_cache_client()
            logger.debug('Requested creation of resource Lock %s', self._name)
            if value != 0:
                self._client.rpush(self._name, *([''] * value))
            self._client.expire(self._name, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            self._lua_release = self._client.register_script(Semaphore.LUA_RELEASE_SCRIPT)
            util.make_stateless_script(self._lua_release)

            self._ref = util.RemoteReference(self._name, client=self._client)

        def __getstate__(self):
            return (self._name, self._max_value, self._client,
                    self._lua_release, self._ref)

        def __setstate__(self, state):
            (self._name, self._max_value, self._client,
            self._lua_release, self._ref) = state

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *args):
            self.release()

        def get_value(self):
            value = self._client.llen(self._name)
            return int(value)

        def acquire(self, block=True):
            if block:
                logger.debug('Requested blocking acquire for lock %s', self._name)
                self._client.blpop([self._name])
                return True
            else:
                logger.debug('Requested non-blocking acquire for lock %s', self._name)
                return self._client.lpop(self._name) is not None

        def release(self):
            logger.debug('Requested release for lock %s', self._name)
            self._lua_release(keys=[self._name],
                            args=[self._max_value],
                            client=self._client)

        def __repr__(self):
            try:
                value = self.get_value()
            except Exception:
                value = 'unknown'
            return '<%s(value=%s)>' % (self.__class__.__name__, value)


    #
    # Semaphore
    #

    class Semaphore(SemLock):
        def __init__(self, value=1):
            super().__init__(value, SEM_VALUE_MAX)


    #
    # Bounded semaphore
    #

    class BoundedSemaphore(SemLock):
        def __init__(self, value=1):
            super().__init__(value, value)


    #
    # Non-recursive lock
    #

    class Lock(SemLock):
        def __init__(self):
            super().__init__(1, 1)
            self.owned = False

        def __setstate__(self, state):
            super().__setstate__(state)
            self.owned = False

        def acquire(self, block=True):
            res = super().acquire(block)
            self.owned = True
            return res

        def release(self):
            super().release()
            self.owned = False


    #
    # Recursive lock
    #

    class RLock(Lock):
        def acquire(self, block=True):
            return self.owned or super().acquire(block)


    #
    # Condition variable
    #

    class Condition:
        def __init__(self, lock=None):
            if lock:
                self._lock = lock
                self._client = util.get_cache_client()
            else:
                self._lock = Lock()
                # help reducing the amount of open clients
                self._client = self._lock._client

            self._notify_handle = 'condition-notify-' + util.get_uuid()
            logger.debug('Requested creation of resource Condition %s', self._notify_handle)
            self._ref = util.RemoteReference(self._notify_handle,
                                            client=self._client)

        def acquire(self):
            return self._lock.acquire()

        def release(self):
            self._lock.release()

        def __enter__(self):
            return self._lock.__enter__()

        def __exit__(self, *args):
            return self._lock.__exit__(*args)

        def wait(self, timeout=None):
            assert self._lock.owned

            # Enqueue the key we will be waiting for until we are notified
            wait_handle = 'condition-wait-' + util.get_uuid()
            res = self._client.rpush(self._notify_handle, wait_handle)

            if not res:
                raise Exception('Condition ({}) could not enqueue waiting key'.format(self._notify_handle))

            # Release lock, wait to get notified, acquire lock
            self.release()
            logger.debug('Waiting for token %s on condition %s', wait_handle, self._notify_handle)
            self._client.blpop([wait_handle], timeout)
            self._client.expire(wait_handle, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            self.acquire()

        def notify(self):
            assert self._lock.owned

            logger.debug('Notify condition %s', self._notify_handle)
            wait_handle = self._client.lpop(self._notify_handle)
            if wait_handle is not None:
                res = self._client.rpush(wait_handle, '')

                if not res:
                    raise Exception('Condition ({}) could not notify one waiting process'.format(self._notify_handle))

        def notify_all(self, msg=''):
            assert self._lock.owned

            logger.debug('Notify all for condition %s', self._notify_handle)
            pipeline = self._client.pipeline(transaction=False)
            pipeline.lrange(self._notify_handle, 0, -1)
            pipeline.delete(self._notify_handle)
            wait_handles, _ = pipeline.execute()

            if len(wait_handles) > 0:
                pipeline = self._client.pipeline(transaction=False)
                for handle in wait_handles:
                    pipeline.rpush(handle, msg)
                results = pipeline.execute()

                if not all(results):
                    raise Exception('Condition ({}) could not notify all waiting processes'.format(self._notify_handle))

        def wait_for(self, predicate, timeout=None):
            result = predicate()
            if result:
                return result
            if timeout is not None:
                endtime = time.monotonic() + timeout
            else:
                endtime = None
                waittime = None
            while not result:
                if endtime is not None:
                    waittime = endtime - time.monotonic()
                    if waittime <= 0:
                        break
                self.wait(waittime)
                result = predicate()
            return result


    #
    # Event
    #

    class Event:
        def __init__(self):
            self._cond = Condition()
            self._client = self._cond._client
            self._flag_handle = 'event-flag-' + util.get_uuid()
            logger.debug('Requested creation of resource Event %s', self._flag_handle)
            self._ref = util.RemoteReference(self._flag_handle,
                                            client=self._client)

        def is_set(self):
            logger.debug('Request event %s is set', self._flag_handle)
            return self._client.get(self._flag_handle) == b'1'

        def set(self):
            with self._cond:
                logger.debug('Request set event %s', self._flag_handle)
                self._client.set(self._flag_handle, '1')
                self._cond.notify_all()

        def clear(self):
            with self._cond:
                logger.debug('Request clear event %s', self._flag_handle)
                self._client.set(self._flag_handle, '0')

        def wait(self, timeout=None):
            with self._cond:
                logger.debug('Request wait for event %s', self._flag_handle)
                self._cond.wait_for(self.is_set, timeout)


    #
    # Barrier
    #

    class Barrier(threading.Barrier):
        def __init__(self, parties, action=None, timeout=None):
            self._cond = Condition()
            self._client = self._cond._client
            uuid = util.get_uuid()
            self._state_handle = 'barrier-state-' + uuid
            self._count_handle = 'barrier-count-' + uuid
            self._ref = util.RemoteReference(referenced=[self._state_handle, self._count_handle],
                                            client=self._client)
            self._action = action
            self._timeout = timeout
            self._parties = parties
            self._state = 0  # 0 = filling, 1 = draining, -1 = resetting, -2 = broken
            self._count = 0

        @property
        def _state(self):
            return int(self._client.get(self._state_handle))

        @_state.setter
        def _state(self, value):
            self._client.set(self._state_handle, value, ex=mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))

        @property
        def _count(self):
            return int(self._client.get(self._count_handle))

        @_count.setter
        def _count(self, value):
            self._client.set(self._count_handle, value, ex=mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))

elif util. mp_config.get_parameter(mp_config.CACHE) == 'memcached':

    #
    # Base class for semaphores and mutexes
    #

    class SemLock:

        def __init__(self, value=1, max_value=1):
            #http://xion.org.pl/2011/12/10/synchronization-through-memcache/
            self._name = 'semlock-' + util.get_uuid()
            self._mutex = 'mutex-' + util.get_uuid()
            self._max_value = max_value
            self._client = util.get_cache_client()
            self._client.set(self._mutex+'-current', 0)
            self._client.set(self._mutex+'-counter',0)

            logger.debug('Requested creation of resource Lock %s', self._name)
            if value != 0:
                self._client.set(self._name, value)
            else:
                self._client.set(self._name, 0)

            self._ref = util.RemoteReference(self._name, client=self._client)

        def __getstate__(self):
            return (self._name, self._mutex, self._max_value, self._client,self._ref)

        def __setstate__(self, state):
            (self._name, self._mutex, self._max_value, self._client, self._ref) = state

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *args):
            self.release()

        def get_value(self):
            return int(self._client.get(self._name))

        def _acquire(self):
            #while not self._client.add(self._mutex, self._mutex+'dummy',noreply=False):
            #    pass
            counter = int(self._client.incr(self._mutex+'-counter',1))
            self._client.set(self._mutex+'-'+str(counter), 'dummy', noreply = False)
            while not self._client.add(self._mutex+'-'+str(counter-1), 'dummy', noreply = False):
                pass

        def _release(self):
            #self._client.delete(self._mutex)
            current = self._client.incr(self._mutex+'-current',1)
            self._client.delete(self._mutex+'-'+str(current))
            

        def acquire(self, block=True):
            if block:
                logger.debug('Requested blocking acquire for lock %s', self._name)
                #self._client.blpop([self._name])
                if int(self._client.get(self._name)) == 0:
                    self._acquire()
                self._client.decr(self._name,1)
                return True
            else:
                logger.debug('Requested non-blocking acquire for lock %s', self._name)
                value = int(self._client.get(self._name))
                if value > 0:
                    self._client.decr(self._name,1)
                return value >= 0

        def release(self):
            logger.debug('Requested release for lock %s', self._name)
            current_value = int(self._client.get(self._name))
            if current_value >= self._max_value:
                #self._release()
                return current_value
            current_value = self._client.incr(self._name,1)
            self._release()
            return current_value
            
            # KEYS[1] - semlock name
            # ARGV[1] - max value
            # return new semlock value
            # only increments its value if
            # it is not above the max value
            """
                local current_value = tonumber(redis.call('llen', KEYS[1]))
                if current_value >= tonumber(ARGV[1]) then
                    return current_value
                end
                redis.call('rpush', KEYS[1], '')
                return current_value + 1
            """

        def __repr__(self):
            try:
                value = self.get_value()
            except Exception:
                value = 'unknown'
            return '<%s(value=%s)>' % (self.__class__.__name__, value)


    #
    # Semaphore
    #

    class Semaphore(SemLock):
        def __init__(self, value=1):
            super().__init__(value, SEM_VALUE_MAX)
            


    #
    # Bounded semaphore
    #

    class BoundedSemaphore(SemLock):
        def __init__(self, value=1):
            super().__init__(value, value)


    #
    # Non-recursive lock
    #

    """ 
    class Lock(SemLock):
        def __init__(self):
            super().__init__(1, 1)
            self.owned = False

        def __setstate__(self, state):
            super().__setstate__(state)
            self.owned = False

        def acquire(self, block=True):
            res = super().acquire(block)
            self.owned = True
            return res

        def release(self):
            super().release()
            self.owned = False 
    """

    #
    # Non-recursive lock
    #

    class Lock:
        
        #http://xion.org.pl/2011/12/10/synchronization-through-memcache/
        def __init__(self):
            self._name = 'lock-' + util.get_uuid()
            self._client = util.get_cache_client()
            self._ref = util.RemoteReference(self._name, client=self._client)
            self._client.set(self._name+'-current', 0)
            self._client.set(self._name+'-counter',0)
            self.owned = False

        def __getstate__(self):
            return (self._name, self._client,self._ref,self.owned)

        def __setstate__(self, state):
            (self._name, self._client,self._ref,self.owned) = state

        def acquire(self,block = True):
            if block:
                #while not self._client.add(self._name, 'dummy', noreply = False):
                #    pass
                counter = int(self._client.incr(self._name+'-counter',1))
                self._client.set(self._name+'-'+str(counter), 'dummy', noreply = False)
                while not self._client.add(self._name+'-'+str(counter-1), 'dummy', noreply = False):
                    pass
            else:
                pass
            self.owned = True
            return True

        def release(self):
            #self._client.delete(self._name)
            next = self._client.incr(self._name+'-current',1)
            self._client.delete(self._name+'-'+str(next))
            self.owned = False
        
        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *args):
            self.release()

    #
    # Recursive lock
    #

    class RLock(Lock):
        def acquire(self, block=True):
            return self.owned or super().acquire(block)


    #
    # Condition variable
    #

    class Condition:
        def __init__(self, lock=None):
            if lock:
                self._lock = lock
                #self._client = util.get_redis_client()
                self._client = util.get_cache_client()
            else:
                self._lock = Lock()
                # help reducing the amount of open clients
                self._client = self._lock._client

            self._mutex = 'mutex-' + util.get_uuid()
            self._notify_handle = 'condition-notify-' + util.get_uuid()
            self._client.set(self._notify_handle, '')
            logger.debug('Requested creation of resource Condition %s', self._notify_handle)
            self._ref = util.RemoteReference(self._notify_handle,
                                            client=self._client)

        def acquire(self):
            return self._lock.acquire()

        def release(self):
            self._lock.release()

        def __enter__(self):
            return self._lock.__enter__()

        def __exit__(self, *args):
            return self._lock.__exit__(*args)

        def _acquire(self, key, timeout = None):
            #self._client.set(self._mutex+key, self._mutex+'dummy')
            self._client.set(self._mutex+key, 0)
            if timeout is not None:
                endtime = time.monotonic() + timeout
            else:
                endtime = None
                waittime = None

            #while not self._client.add(self._mutex+key, self._mutex+'dummy',noreply=False):
            while 0 == int(self._client.get(self._mutex+key)):
                if endtime is not None:
                    waittime = endtime - time.monotonic()
                    if waittime <= 0:
                        #print('Timeout triggered something went wrong')
                        self._client.incr(self._mutex+key,1)
                        break
            self._client.delete(self._mutex+key)
            #print('acquire ' + str(time.time()))
            

        def _release(self, key):
            #self._client.delete(self._mutex+key)
            self._client.incr(self._mutex+key,1)
            
        def wait(self, timeout=None):
            assert self._lock.owned

            # Enqueue the key we will be waiting for until we are notified

            wait_handle = 'condition-wait-' + util.get_uuid()
            self._client.set(wait_handle, 0)
            self._client.append(self._notify_handle, wait_handle+',')
            #if not res:
            #    raise Exception('Condition ({}) could not enqueue waiting key'.format(self._notify_handle))

            # Release lock, wait to get notified, acquire lock
            self.release()
            logger.debug('Waiting for token %s on condition %s', wait_handle, self._notify_handle)

            temp = int(self._client.get(wait_handle))
            if temp == 0:
                self._acquire(wait_handle,timeout)
                self._client.decr(wait_handle,1)
            self.acquire()
            

        def notify(self):
            assert self._lock.owned

            logger.debug('Notify condition %s', self._notify_handle)
            temp = self._client.get(self._notify_handle).decode('ascii').split(',')
            wait_handle = temp[0]
            self._client.replace(self._notify_handle, temp[1:])

            if wait_handle is not None:
                
                self._client.incr(wait_handle,1)
                self._release(wait_handle)

                if not res:
                    raise Exception('Condition ({}) could not notify one waiting process'.format(self._notify_handle))

        def notify_all(self, msg=''):
            assert self._lock.owned

            logger.debug('Notify all for condition %s', self._notify_handle)
            wait_handles = self._client.get(self._notify_handle)

            if wait_handles != None :
                self._client.set(self._notify_handle,'')
                wait_handles = wait_handles.decode('ascii')
                if wait_handles != '':
                    for wait_handle in wait_handles.split(',')[:-1]:
                        self._client.incr(wait_handle,1)
                        self._release(wait_handle)

                #if not all(results):
                #    raise Exception('Condition ({}) could not notify all waiting processes'.format(self._notify_handle))

        def wait_for(self, predicate, timeout=None):
            result = predicate()
            if result:
                return result
            if timeout is not None:
                endtime = time.monotonic() + timeout
            else:
                endtime = None
                waittime = None
            while not result:
                if endtime is not None:
                    waittime = endtime - time.monotonic()
                    if waittime <= 0:
                        break
                self.wait(waittime)
                result = predicate()
            return result


    #
    # Event
    #

    class Event:
        def __init__(self):
            self._cond = Condition()
            self._client = self._cond._client
            
            self._flag_handle = 'event-flag-' + util.get_uuid()
            logger.debug('Requested creation of resource Event %s', self._flag_handle)
            self._ref = util.RemoteReference(self._flag_handle,client=self._client)

        def is_set(self):
            logger.debug('Request event %s is set', self._flag_handle)
            return self._client.get(self._flag_handle) == b'1'

        def set(self):
            with self._cond:
                logger.debug('Request set event %s', self._flag_handle)
                self._client.set(self._flag_handle, '1')
                self._cond.notify_all()

        def clear(self):
            with self._cond:
                logger.debug('Request clear event %s', self._flag_handle)
                self._client.set(self._flag_handle, '0')

        def wait(self, timeout=None):
            with self._cond:
                logger.debug('Request wait for event %s', self._flag_handle)
                self._cond.wait_for(self.is_set, timeout)


    #
    # Barrier
    #

    class Barrier(threading.Barrier):
        def __init__(self, parties, action=None, timeout=None):
            self._cond = Condition()
            self._client = self._cond._client
            
            uuid = util.get_uuid()
            self._state_handle = 'barrier-state-' + uuid
            self._count_handle = 'barrier-count-' + uuid
            self._ref = util.RemoteReference(referenced=[self._state_handle, self._count_handle], client=self._client)
            self._action = action
            self._timeout = timeout
            self._parties = parties
            self._state = 0  # 0 = filling, 1 = draining, -1 = resetting, -2 = broken
            self._count = 0

        @property
        def _state(self):
            return int(self._client.get(self._state_handle))

        @_state.setter
        def _state(self, value):
            self._client.set(self._state_handle, value)

        @property
        def _count(self):
            return int(self._client.get(self._count_handle))

        @_count.setter
        def _count(self, value):
            self._client.set(self._count_handle, value)

else: 
    # Queue based manager

    #
    # Base class for semaphores and mutexes
    #

    class SemLock:
        # KEYS[1] - semlock name
        # ARGV[1] - max value
        # return new semlock value
        # only increments its value if
        # it is not above the max value

        def __init__(self, value=1, max_value=1):
            self._name = 'semlock-' + util.get_uuid()
            self._max_value = max_value
            self._queue = queues.Queue()
            logger.debug('Requested creation of resource Lock %s', self._name)
            if value != 0:
                n = 0
                while n !=value:
                    self._queue.put(0)
                    n+=1
            #self._ref = util.RemoteReference(self._name, client=self._client)

        def __getstate__(self):
            return (self._name, self._max_value, self._queue)#, self._ref)

        def __setstate__(self, state):
            (self._name, self._max_value, self._queue) = state#,  self._ref) = state

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *args):
            self.release()

        def get_value(self):
            value = self._queue.qsize()
            return int(value)

        def acquire(self, block=True):
            if block:
                logger.debug('Requested blocking acquire for lock %s', self._name)
                self._queue.get(block)
                return True
            else:
                logger.debug('Requested non-blocking acquire for lock %s', self._name)
                return self._queue.get()

        def release(self, n=1):
            logger.debug('Requested release for lock %s', self._name)
            c = 0
            while c !=n:
                self._queue.put(0)
                c+=1

        def __repr__(self):
            try:
                value = self.get_value()
            except Exception:
                value = 'unknown'
            return '<%s(value=%s)>' % (self.__class__.__name__, value)


    #
    # Semaphore
    #

    class Semaphore(SemLock):
        def __init__(self, value=1):
            super().__init__(value, SEM_VALUE_MAX)

    #
    # Bounded semaphore
    #

    class BoundedSemaphore(SemLock):
        def __init__(self, value=1):
            super().__init__(value, value)


    #
    # Non-recursive lock
    #

    class Lock(SemLock):
        def __init__(self):
            super().__init__(1, 1)
            self.owned = False

        def __setstate__(self, state):
            super().__setstate__(state)
            self.owned = False

        def acquire(self, block=True):
            res = super().acquire(block)
            self.owned = True
            return res

        def release(self):
            super().release()
            self.owned = False


    #
    # Recursive lock
    #

    class RLock(Lock):
        def acquire(self, block=True):
            return self.owned or super().acquire(block)


    #
    # Condition variable
    #

    class Condition:
        def __init__(self, lock=None):
            if lock:
                self._lock = lock
                #self._client = util.get_redis_client()
                self._queue = queues.Queue()
                self._queue_wait = queues.Queue()
            else:
                self._lock = Lock()
                # help reducing the amount of open clients
                #self._client = self._lock._client
                self._queue = queues.Queue()
                self._queue_wait = queues.Queue()

            self._notify_handle = 'condition-notify-' + util.get_uuid()
            logger.debug('Requested creation of resource Condition %s', self._notify_handle)
            #self._ref = util.RemoteReference(self._notify_handle,client=self._client)

        def acquire(self):
            return self._lock.acquire()

        def release(self):
            self._lock.release()

        def __enter__(self):
            return self._lock.__enter__()

        def __exit__(self, *args):
            return self._lock.__exit__(*args)

        def wait(self, timeout=None):
            assert self._lock.owned

            # Enqueue the key we will be waiting for until we are notified
            wait_handle = 'condition-wait-' + util.get_uuid()
            self._queue.put(wait_handle)

            #if not res:
            #    raise Exception('Condition ({}) could not enqueue waiting key'.format(self._notify_handle))

            # Release lock, wait to get notified, acquire lock
            self.release()
            logger.debug('Waiting for token %s on condition %s', wait_handle, self._notify_handle)
            self._queue_wait.get(block= True, timeout = timeout)
            self.acquire()

        def notify(self):
            assert self._lock.owned

            logger.debug('Notify condition %s', self._notify_handle)
            wait_handle = self._queue.get()
            if wait_handle is not None:
                self._queue_wait.put(wait_handle)
                #if not res:
                #    raise Exception('Condition ({}) could not notify one waiting process'.format(self._notify_handle))

        def notify_all(self, msg=''):
            assert self._lock.owned

            logger.debug('Notify all for condition %s', self._notify_handle)
            wait_handles = []
            while not self._queue.empty():
                wait_handles.append(self._queue.get())

            if len(wait_handles) > 0:
                for handle in wait_handles:
                    self._queue_wait.put(handle+msg)

                #if not all(results):
                #    raise Exception('Condition ({}) could not notify all waiting processes'.format(self._notify_handle))

        def wait_for(self, predicate, timeout=None):
            result = predicate()
            if result:
                return result
            if timeout is not None:
                endtime = time.monotonic() + timeout
            else:
                endtime = None
                waittime = None
            while not result:
                if endtime is not None:
                    waittime = endtime - time.monotonic()
                    if waittime <= 0:
                        break
                self.wait(waittime)
                result = predicate()
            return result


    #
    # Event
    #

    class Event:
        def __init__(self):
            self._cond = Condition()
            #self._client = self._cond._client
            self._queue = queues.Queue()
            self._flag_handle = 'event-flag-' + util.get_uuid()
            logger.debug('Requested creation of resource Event %s', self._flag_handle)
            #self._ref = util.RemoteReference(self._flag_handle,client=self._client)

        def is_set(self):
            logger.debug('Request event %s is set', self._flag_handle)
            #return self._client.get(self._flag_handle) == b'1'
            return not self._queue.empty()


        def set(self):
            with self._cond:
                logger.debug('Request set event %s', self._flag_handle)
                #self._client.set(self._flag_handle, '1')
                self._queue.put(1)
                self._cond.notify_all()

        def clear(self):
            with self._cond:
                logger.debug('Request clear event %s', self._flag_handle)
                #self._client.set(self._flag_handle, '0')
                self._queue.get()

        def wait(self, timeout=None):
            with self._cond:
                logger.debug('Request wait for event %s', self._flag_handle)
                self._cond.wait_for(self.is_set, timeout)


    #
    # Barrier
    #

    class Barrier(threading.Barrier):
        def __init__(self, parties, action=None, timeout=None):
            self._cond = Condition()
            #self._client = self._cond._client
            self._queue_state = queues.Queue()
            self._queue_count = queues.Queue()
            uuid = util.get_uuid()
            self._state_handle = 'barrier-state-' + uuid
            self._count_handle = 'barrier-count-' + uuid
            #self._ref = util.RemoteReference(referenced=[self._state_handle, self._count_handle],client=self._client)
            self._action = action
            self._timeout = timeout
            self._parties = parties
            self._state = 0  # 0 = filling, 1 = draining, -1 = resetting, -2 = broken
            self._count = 0

        @property
        def _state(self):
            return int(self._queue_state.get())

        @_state.setter
        def _state(self, value):
            self._queue_state.put(value)

        @property
        def _count(self):
            return int(self._queue_count.get())

        @_count.setter
        def _count(self, value):
            self._queue_count.put(value)