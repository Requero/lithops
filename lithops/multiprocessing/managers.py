#
# Module providing the `SyncManager` class for dealing
# with shared objects
#
# multiprocessing/managers.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Cloudlab URV
#

#
# Imports
#


import inspect
import cloudpickle
import logging
import json

from . import pool
from . import synchronize
from . import queues
from . import util
from . import config as mp_config

logger = logging.getLogger(__name__)

_builtin_types = {
    'list',
    'dict',
    'Namespace',
    'Lock',
    'RLock',
    'Semaphore',
    'BoundedSemaphore',
    'Condition',
    'Event',
    'Barrier',
    'Queue',
    'Value',
    'Array',
    'JoinableQueue',
    'SimpleQueue',
    'Pool'
}


#
# Helper functions
#



if 'redis'in util. mp_config.get_parameter(mp_config.CACHE): 
    def deslice(slic: slice):
        start = slic.start
        end = slic.stop
        step = slic.step

        if start is None:
            start = 0
        if end is None:
            end = -1
        elif start == end or end == 0:
            return None, None, None
        else:
            end -= 0

        return start, end, step
    import redis
    #
    # Definition of BaseManager
    #

    class BaseManager:
        """
        Base class for managers
        """
        _registry = {}

        def __init__(self, address=None, authkey=None, serializer='pickle', ctx=None):
            self._client = util.get_cache_client()
            self._managing = False
            self._mrefs = []

        def get_server(self):
            pass

        def connect(self):
            pass

        def start(self, initializer=None, initargs=()):
            self._managing = True

        def _create(self, typeid, *args, **kwds):
            """
            Create a new shared object; return the token and exposed tuple
            """
            pass

        def join(self, timeout=None):
            pass

        def _number_of_objects(self):
            """
            Return the number of shared objects
            """
            return len(self._mrefs)

        def __enter__(self):
            self.start()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.shutdown()

        def shutdown(self):
            if self._managing:
                for ref in self._mrefs:
                    ref.collect()
                self._mrefs = []
                self._managing = False

        @classmethod
        def register(cls, typeid, proxytype=None, callable=None, exposed=None,
                    method_to_typeid=None, create_method=True, can_manage=True):
            """
            Register a typeid with the manager type
            """

            def temp(self, *args, **kwargs):
                logger.debug('requesting creation of a shared %r object', typeid)

                if typeid in _builtin_types:
                    proxy = proxytype(*args, **kwargs)
                else:
                    proxy = GenericProxy(typeid, proxytype, *args, **kwargs)

                if self._managing and can_manage and hasattr(proxy, '_ref'):
                    proxy._ref.managed = True
                    self._mrefs.append(proxy._ref)
                return proxy

            temp.__name__ = typeid
            setattr(cls, typeid, temp)


    #
    # Definition of BaseProxy
    #

    class BaseProxy:
        """
        A base for proxies of shared objects
        """

        def __init__(self, typeid, serializer=None):
            self._typeid = typeid
            # object id
            self._oid = '{}-{}'.format(typeid, util.get_uuid())

            self._pickler = cloudpickle
            self._client = util.get_cache_client()
            self._ref = util.RemoteReference(self._oid, client=self._client)

        def __repr__(self):
            return '<{} object, typeid={}, key={}>'.format(type(self).__name__, self._typeid, self._oid)

        def __str__(self):
            """
            Return representation of the referent (or a fall-back if that fails)
            """
            return repr(self)


    #
    # Types/callables which we will register with SyncManager
    #

    class GenericProxy(BaseProxy):
        def __init__(self, typeid, klass, *args, **kwargs):
            super().__init__(typeid)
            self._klass = klass
            self._init_args = (args, kwargs)

            obj = self._after_fork()
            self._init_obj(obj)

        def _after_fork(self):
            args, kwargs = self._init_args
            obj = self._klass(*args, **kwargs)

            for attr_name in (attr for attr in dir(obj) if inspect.ismethod(getattr(obj, attr))):
                wrap = MethodWrapper(self, attr_name, obj)
                setattr(self, attr_name, wrap)

            return obj

        def _init_obj(self, obj):

            if not hasattr(obj, '__shared__'):
                shared = list(vars(obj).keys())
            else:
                shared = obj.__shared__

            pipeline = self._client.pipeline()
            for attr_name in shared:
                attr = getattr(obj, attr_name)
                attr_bin = self._pickler.dumps(attr)
                pipeline.hset(self._oid, attr_name, attr_bin)
            pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            
            pipeline.execute()

        def __getstate__(self):
            return {
                '_typeid': self._typeid,
                '_oid': self._oid,
                '_pickler': self._pickler,
                '_client': self._client,
                '_ref': self._ref,
                '_klass': self._klass,
                '_init_args': self._init_args,
            }

        def __setstate__(self, state):
            self._typeid = state['_typeid']
            self._oid = state['_oid']
            self._pickler = state['_pickler']
            self._client = state['_client']
            self._ref = state['_ref']
            self._klass = state['_klass']
            self._init_args = state['_init_args']
            self._after_fork()


    class MethodWrapper:
        def __init__(self, proxy, attr_name, shared_object):
            self._attr_name = attr_name
            self._shared_object = shared_object
            self._proxy = proxy

        def __call__(self, *args, **kwargs):
            attrs = self._proxy._client.hgetall(self._proxy._oid)

            hashes = {}

            for attr_name, attr_bin in attrs.items():
                attr_name = attr_name.decode('utf-8')
                attr = self._proxy._pickler.loads(attr_bin)
                hashes[attr_name] = hash(attr_bin)
                setattr(self._shared_object, attr_name, attr)

            attr = getattr(self._shared_object, self._attr_name)
            if callable(attr):
                result = attr.__call__(*args, **kwargs)
            else:
                result = attr

            if not hasattr(self._shared_object, '__shared__'):
                shared = list(vars(self._shared_object).keys())
            else:
                shared = self._shared_object.__shared__

            pipeline = self._proxy._client.pipeline()
            for attr_name in shared:
                attr = getattr(self._shared_object, attr_name)
                attr_bin = self._proxy._pickler.dumps(attr)
                if hash(attr_bin) != hashes[attr_name]:
                    pipeline.hset(self._proxy._oid, attr_name, attr_bin)
            pipeline.expire(self._proxy._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            pipeline.execute()

            return result


    class ListProxy(BaseProxy):
        # NOTE: list slices should return an instance of a ListProxy
        #       or a native python list?
        #
        #          A = ListProxy([1, 2, 3])
        #          B = A[:2]
        #          C = A + [4, 5]
        #          D = A * 3
        #
        #        Following the multiprocessing implementation, lists (B,
        #        C, D) are plain python lists while A is the only ListProxy.
        #        This could cause problems like these:
        #
        #          A = A[2:]
        #
        #        with A being a python list after executing that line.
        #
        #        Current implementation is the same as multiprocessing

        # KEYS[1] - key to extend
        # KEYS[2] - key to extend with
        # ARGV[1] - number of repetitions
        # A = A + B * C
        LUA_EXTEND_LIST_SCRIPT = """
            local values = redis.call('LRANGE', KEYS[2], 0, -1)
            if #values == 0 then
                return
            else
                for i=1,tonumber(ARGV[1]) do
                    redis.call('RPUSH', KEYS[1], unpack(values))
                end
            end
        """

        def __init__(self, iterable=None):
            super().__init__('list')
            self._lua_extend_list = self._client.register_script(ListProxy.LUA_EXTEND_LIST_SCRIPT)
            util.make_stateless_script(self._lua_extend_list)

            if iterable is not None:
                self.extend(iterable)

        def __setitem__(self, i, obj):
            if isinstance(i, int) or hasattr(i, '__index__'):
                idx = i.__index__()
                serialized = self._pickler.dumps(obj)
                try:
                    pipeline = self._client.pipeline()
                    pipeline.lset(self._oid, idx, serialized)
                    pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                    pipeline.execute()
                except redis.exceptions.ResponseError:
                    # raised when index >= len(self)
                    raise IndexError('list assignment index out of range')

            elif isinstance(i, slice):  # TODO: step
                start, end, step = deslice(i)
                if start is None:
                    return

                if end < 0:
                    end = len(self) + end +1

                pipeline = self._client.pipeline(transaction=False)
                try:
                    iterable = iter(obj)
                    for j in range(start, end):
                        obj = next(iterable)
                        serialized = self._pickler.dumps(obj)
                        pipeline.lset(self._oid, j, serialized)
                except StopIteration:
                    pass
                except redis.exceptions.ResponseError:
                    # raised when index >= len(self)
                    pipeline.execute()
                    self.extend(iterable)
                    return
                except TypeError:
                    raise TypeError('can only assign an iterable')
                pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                pipeline.execute()
            else:
                raise TypeError('list indices must be integers '
                                'or slices, not {}'.format(type(i)))

        def __getitem__(self, i):
            if isinstance(i, int) or hasattr(i, '__index__'):
                idx = i.__index__()
                pipeline = self._client.pipeline()
                pipeline.lindex(self._oid, idx)
                pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                serialized, _ = pipeline.execute()
                if serialized is not None:
                    return self._pickler.loads(serialized)
                raise IndexError('list index out of range')

            elif isinstance(i, slice):  # TODO: step
                start, end, step = deslice(i)
                if start is None:
                    return []
                pipeline = self._client.pipeline()
                pipeline.lrange(self._oid, start, end)
                pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                serialized, _ = pipeline.execute()
                unserialized = [self._pickler.loads(obj) for obj in serialized]
                return unserialized
                # return type(self)(unserialized)
            else:
                raise TypeError('list indices must be integers '
                                'or slices, not {}'.format(type(i)))

        def extend(self, iterable):
            if isinstance(iterable, type(self)):
                self._extend_same_type(iterable, 1)
            else:
                if iterable != []:
                    values = map(self._pickler.dumps, iterable)
                    pipeline = self._client.pipeline()
                    pipeline.rpush(self._oid, *values)
                    pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                    pipeline.execute()

        def _extend_same_type(self, listproxy, repeat=1):
            self._lua_extend_list(keys=[self._oid, listproxy._oid],
                                args=[repeat],
                                client=self._client)

        def append(self, obj):
            serialized = self._pickler.dumps(obj)
            pipeline = self._client.pipeline()
            pipeline.rpush(self._oid, serialized)
            pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            pipeline.execute()

        def pop(self, index=None):
            if index is None:
                pipeline = self._client.pipeline()
                pipeline.rpop(self._oid)
                pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                serialized, _ = pipeline.execute()

                if serialized is not None:
                    return self._pickler.loads(serialized)
            else:
                item = self[index]
                sentinel = util.get_uuid()
                self[index] = sentinel
                self.remove(sentinel)
                return item

        def __deepcopy__(self, memo):
            selfcopy = type(self)()

            # We should test the DUMP/RESTORE strategy 
            # although it has serialization costs
            selfcopy._extend_same_type(self)

            memo[id(self)] = selfcopy
            return selfcopy

        def __add__(self, x):
            # FIXME: list only allows concatenation to other list objects
            #        (altough it can now be extended by iterables)
            # newlist = deepcopy(self)
            # return newlist.__iadd__(x)
            return self[:] + x

        def __iadd__(self, x):
            # FIXME: list only allows concatenation to other list objects
            #        (altough it can now be extended by iterables)
            self.extend(x)
            return self

        def __mul__(self, n):
            if not isinstance(n, int):
                raise TypeError("TypeError: can't multiply sequence"
                                " by non-int of type {}".format(type(n)))
            if n < 1:
                # return type(self)()
                return []
            else:
                # newlist = type(self)()
                # newlist._extend_same_type(self, repeat=n)
                # return newlist
                return self[:] * n

        def __rmul__(self, n):
            return self.__mul__(n)

        def __imul__(self, n):
            if not isinstance(n, int):
                raise TypeError("TypeError: can't multiply sequence"
                                " by non-int of type {}".format(type(n)))
            if n > 1:
                self._extend_same_type(self, repeat=n - 1)
            return self

        def __len__(self):
            return self._client.llen(self._oid)

        def remove(self, obj):
            serialized = self._pickler.dumps(obj)
            pipeline = self._client.pipeline()
            pipeline.lrem(self._oid, 1, serialized)
            pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            pipeline.execute()
            return self

        def __delitem__(self, i):
            sentinel = util.get_uuid()
            self[i] = sentinel
            self.remove(sentinel)

        def tolist(self):
            serialized = self._client.lrange(self._oid, 0, -1)
            unserialized = [self._pickler.loads(obj) for obj in serialized]
            return unserialized

        # The following methods can't be (properly) implemented on Redis
        # To still provide the functionality, the list is fetched
        # entirely, operated in-memory and then put back to Redis

        def reverse(self):
            rev = reversed(self[:])
            self._client.delete(self._oid)
            self.extend(rev)
            return self

        def sort(self, key=None, reverse=False):
            sortd = sorted(self[:], key=key, reverse=reverse)
            self._client.delete(self._oid)
            self.extend(sortd)
            return self

        def index(self, obj, start=0, end=-1):
            return self[:].index(obj, start, end)

        def count(self, obj):
            return self[:].count(obj)

        def insert(self, index, obj):
            new_list = self[:]
            new_list.insert(index, obj)
            self._client.delete(self._oid)
            self.extend(new_list)


    class DictProxy(BaseProxy):

        def __init__(self, *args, **kwargs):
            super().__init__('dict')
            self.update(*args, **kwargs)

        def __setitem__(self, k, v):
            serialized = self._pickler.dumps(v)
            pipeline = self._client.pipeline()
            pipeline.hset(self._oid, k, serialized)
            pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            pipeline.execute()

        def __getitem__(self, k):
            pipeline = self._client.pipeline()
            pipeline.hget(self._oid, k)
            pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            serialized, _ = pipeline.execute()
            if serialized is None:
                raise KeyError(k)

            return self._pickler.loads(serialized)

        def __delitem__(self, k):
            pipeline = self._client.pipeline()
            pipeline.hdel(self._oid, k)
            pipeline.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            res, _ = pipeline.execute()

            if res == 0:
                raise KeyError(k)

        def __contains__(self, k):
            return self._client.hexists(self._oid, k)

        def __len__(self):
            return self._client.hlen(self._oid)

        def __iter__(self):
            return iter(self.keys())

        def get(self, k, default=None):
            try:
                v = self.__getitem__(k)
            except KeyError:
                return default
            else:
                return v

        def pop(self, k, default=None):
            try:
                v = self.__getitem__(k)
            except KeyError:
                return default
            else:
                self.__delitem__(k)
                return v

        def popitem(self):
            try:
                key = self.keys()[0]
                item = (key, self.__getitem__(key))
                self.__delitem__(key)
                return item
            except IndexError:
                raise KeyError('popitem(): dictionary is empty')

        def setdefault(self, k, default=None):
            serialized = self._pickler.dumps(default)
            res = self._client.hsetnx(self._oid, k, serialized)
            if res == 1:
                return default
            else:
                return self.__getitem__(k)

        def update(self, *args, **kwargs):
            items = []
            if args != ():
                if len(args) > 1:
                    raise TypeError('update expected at most'
                                    ' 1 arguments, got {}'.format(len(args)))
                try:
                    for k in args[0].keys():
                        items.extend((k, self._pickler.dumps(args[0][k])))
                except:
                    try:
                        items = []  # just in case
                        for k, v in args[0]:
                            items.extend((k, self._pickler.dumps(v)))
                    except:
                        raise TypeError(type(args[0]))

            for k in kwargs.keys():
                items.extend((k, self._pickler.dumps(kwargs[k])))

            if len(items) > 0:
                self._client.execute_command('HMSET', self._oid, *items)
                self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))

        def keys(self):
            return [k.decode() for k in self._client.hkeys(self._oid)]

        def values(self):
            return [self._pickler.loads(v) for v in self._client.hvals(self._oid)]

        def items(self):
            raw_dict = self._client.hgetall(self._oid)
            items = []
            for k, v in raw_dict.items():
                items.append((k.decode(), self._pickler.loads(v)))
            return items

        def clear(self):
            self._client.delete(self._oid)

        def copy(self):
            # TODO: use lua script
            return type(self)(self.items())

        def todict(self):
            raw_dict = self._client.hgetall(self._oid)
            py_dict = {}
            for k, v in raw_dict.items():
                py_dict[k.decode()] = self._pickler.loads(v)
            return py_dict


    class NamespaceProxy(BaseProxy):
        def __init__(self, **kwargs):
            super().__init__('Namespace')
            DictProxy.update(self, **kwargs)

        def __getattr__(self, k):
            if k[0] == '_':
                return object.__getattribute__(self, k)
            try:
                return DictProxy.__getitem__(self, k)
            except KeyError:
                raise AttributeError(k)

        def __setattr__(self, k, v):
            if k[0] == '_':
                return object.__setattr__(self, k, v)
            DictProxy.__setitem__(self, k, v)

        def __delattr__(self, k):
            if k[0] == '_':
                return object.__delattr__(self, k)
            try:
                return DictProxy.__delitem__(self, k)
            except KeyError:
                raise AttributeError(k)

        def _todict(self):
            return DictProxy.todict(self)


    class ValueProxy(BaseProxy):
        def __init__(self, typecode='Any', value=None, lock=True):
            super().__init__('Value({})'.format(typecode))
            if value is not None:
                self.set(value)

        def get(self):
            serialized = self._client.get(self._oid)
            return self._pickler.loads(serialized)

        def set(self, value):
            serialized = self._pickler.dumps(value)
            self._client.set(self._oid, serialized, ex=mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))

        value = property(get, set)


    class ArrayProxy(ListProxy):
        def __init__(self, typecode, sequence, lock=True):
            super().__init__(sequence)

    
    #
    # Definition of SyncManager
    #

    class SyncManager(BaseManager):
        """
        Subclass of `BaseManager` which supports a number of shared object types.

        The types registered are those intended for the synchronization
        of threads, plus `dict`, `list` and `Namespace`.

        The `multiprocessing.Manager()` function creates started instances of
        this class.
        """

    SyncManager.register('list', ListProxy)
    SyncManager.register('dict', DictProxy)
    SyncManager.register('Namespace', NamespaceProxy)
    SyncManager.register('Lock', synchronize.Lock)
    SyncManager.register('RLock', synchronize.RLock)
    SyncManager.register('Semaphore', synchronize.Semaphore)
    SyncManager.register('BoundedSemaphore', synchronize.BoundedSemaphore)
    SyncManager.register('Condition', synchronize.Condition)
    SyncManager.register('Event', synchronize.Event)
    SyncManager.register('Barrier', synchronize.Barrier)
    SyncManager.register('Queue', queues.Queue)
    SyncManager.register('SimpleQueue', queues.SimpleQueue)
    SyncManager.register('JoinableQueue', queues.JoinableQueue)
    SyncManager.register('Value', ValueProxy)
    SyncManager.register('Array', ArrayProxy)
    SyncManager.register('Pool', pool.Pool, can_manage=False)

elif 'memcached' in util. mp_config.get_parameter(mp_config.CACHE):
    def deslice(slic: slice):
        start = slic.start
        end = slic.stop
        step = slic.step

        if start is None:
            start = 0
        if end is None:
            end = -1
        elif start == end or end == 0:
            return None, None, None
        
        return start, end, step
    #
    # Definition of BaseManager
    #

    class BaseManager:
        """
        Base class for managers
        """
        _registry = {}

        def __init__(self, address=None, authkey=None, serializer='pickle', ctx=None):
            self._client = util.get_cache_client()
            self._managing = False
            self._mrefs = []

        def get_server(self):
            pass

        def connect(self):
            pass

        def start(self, initializer=None, initargs=()):
            self._managing = True

        def _create(self, typeid, *args, **kwds):
            """
            Create a new shared object; return the token and exposed tuple
            """
            pass

        def join(self, timeout=None):
            pass

        def _number_of_objects(self):
            """
            Return the number of shared objects
            """
            return len(self._mrefs)

        def __enter__(self):
            self.start()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.shutdown()

        def shutdown(self):
            if self._managing:
                for ref in self._mrefs:
                    ref.collect()
                self._mrefs = []
                self._managing = False

        @classmethod
        def register(cls, typeid, proxytype=None, callable=None, exposed=None,
                    method_to_typeid=None, create_method=True, can_manage=True):
            """
            Register a typeid with the manager type
            """

            def temp(self, *args, **kwargs):
                logger.debug('requesting creation of a shared %r object', typeid)

                if typeid in _builtin_types:
                    proxy = proxytype(*args, **kwargs)
                else:
                    proxy = GenericProxy(typeid, proxytype, *args, **kwargs)

                if self._managing and can_manage and hasattr(proxy, '_ref'):
                    proxy._ref.managed = True
                    self._mrefs.append(proxy._ref)
                return proxy

            temp.__name__ = typeid
            setattr(cls, typeid, temp)


    #
    # Definition of BaseProxy
    #

    class BaseProxy:
        """
        A base for proxies of shared objects
        """

        def __init__(self, typeid, serializer=None):
            self._typeid = typeid
            # object id
            self._uuid = util.get_uuid()
            self._oid = '{}-{}'.format(typeid, self._uuid)
            self._oid = self._oid.replace(" ", "")
            self._pickler = cloudpickle
            self._client = util.get_cache_client()
            self._ref = util.RemoteReference(self._oid, client=self._client)

        def __repr__(self):
            return '<{} object, typeid={}, key={}>'.format(type(self).__name__, self._typeid, self._oid)

        def __str__(self):
            """
            Return representation of the referent (or a fall-back if that fails)
            """
            return repr(self)


    #
    # Types/callables which we will register with SyncManager
    #

    class GenericProxy(BaseProxy):
        def __init__(self, typeid, klass, *args, **kwargs):
            super().__init__(typeid)
            self._klass = klass
            self._init_args = (args, kwargs)

            obj = self._after_fork()
            self._init_obj(obj)

        def _after_fork(self):
            args, kwargs = self._init_args
            obj = self._klass(*args, **kwargs)

            for attr_name in (attr for attr in dir(obj) if inspect.ismethod(getattr(obj, attr))):
                wrap = MethodWrapper(self, attr_name, obj)
                setattr(self, attr_name, wrap)

            return obj

        def _init_obj(self, obj):

            if not hasattr(obj, '__shared__'):
                shared = list(vars(obj).keys())
            else:
                shared = obj.__shared__

            temp = {}
            for attr_name in shared:
                attr = getattr(obj, attr_name)
                attr_bin = attr#self._pickler.dumps(attr)
                temp[attr_name] = attr_bin
            
            self._client.set(self._oid, self._pickler.dumps(temp))
            

        def __getstate__(self):
            return {
                '_typeid': self._typeid,
                '_oid': self._oid,
                '_uuid': self._uuid,
                '_pickler': self._pickler,
                '_client': self._client,
                '_ref': self._ref,
                '_klass': self._klass,
                '_init_args': self._init_args,
            }

        def __setstate__(self, state):
            self._typeid = state['_typeid']
            self._oid = state['_oid']
            self._uuid = state['_uuid']
            self._pickler = state['_pickler']
            self._client = state['_client']
            self._ref = state['_ref']
            self._klass = state['_klass']
            self._init_args = state['_init_args']
            self._after_fork()


    class MethodWrapper:
        def __init__(self, proxy, attr_name, shared_object):
            self._attr_name = attr_name
            self._shared_object = shared_object
            self._proxy = proxy

        def __call__(self, *args, **kwargs):

            attrs = self._proxy._pickler.loads(self._proxy._client.get(self._proxy._oid))

            hashes = {}
            
            for attr_name, attr_bin in attrs.items():
                #attr_name = attr_name.decode('utf-8')
                attr = attr_bin#self._proxy._pickler.loads(attr_bin)
                hashes[attr_name] = hash(self._proxy._pickler.dumps(attr_bin))
                setattr(self._shared_object, attr_name, attr)

            attr = getattr(self._shared_object, self._attr_name)
            if callable(attr):
                result = attr.__call__(*args, **kwargs)
            else:
                result = attr

            if not hasattr(self._shared_object, '__shared__'):
                shared = list(vars(self._shared_object).keys())
            else:
                shared = self._shared_object.__shared__

            temp = {}
            for attr_name in shared:
                attr = getattr(self._shared_object, attr_name)
                attr_bin = attr#self._proxy._pickler.dumps(attr)
                #if hash(self._proxy._pickler.dumps(attr_bin)) != hashes[attr_name]:
                temp[attr_name] = attr_bin

            self._proxy._client.set(self._proxy._oid, self._proxy._pickler.dumps(temp))
            
            return result


    class ListProxy(BaseProxy):

        def __init__(self, iterable=None):
            super().__init__('list')
            
            if iterable is not None:
                self._client.set(self._oid, self._pickler.dumps(list(iterable)))
            else: 
                self._client.set(self._oid, self._pickler.dumps([]))

        # The following methods can't be (properly) implemented on Redis
        # The list is fetched entirely, operated in-memory and then put back to Memcached

        def __setitem__(self, i, obj):
            if isinstance(i, int) or hasattr(i, '__index__'):
                idx = i.__index__()
                current = self._pickler.loads(self._client.get(self._oid))
                try:
                    current[i] = obj
                    self._client.set(self._oid, self._pickler.dumps(current))
                except IndexError:
                    # raised when index >= len(self)
                    raise IndexError('list assignment index out of range')

            elif isinstance(i, slice):  # TODO: step
                start, end, step = deslice(i)
                if start is None:
                    return

                try:
                    iterable = iter(obj)
                    current = self._pickler.loads(self._client.get(self._oid))
                    if end < 0:
                        end = len(current) + end +1

                    for j in range(start, end):
                        obj = next(iterable)
                        current[j]=obj

                    self._client.set(self._oid, self._pickler.dumps(current))
                except StopIteration:
                    pass
                except TypeError:
                    raise TypeError('can only assign an iterable')
            else:
                raise TypeError('list indices must be integers '
                                'or slices, not {}'.format(type(i)))

        def __getitem__(self, i):
            if isinstance(i, int) or hasattr(i, '__index__'):
                idx = i.__index__()
                serialized = self._pickler.loads(self._client.get(self._oid))
                if serialized is not None:
                    return serialized[i]
                raise IndexError('list index out of range')

            elif isinstance(i, slice):  # TODO: step
                start, end, step = deslice(i)
                if start is None:
                    return []
                serialized = self._pickler.loads(self._client.get(self._oid))
                if end < 0:
                    end = len(serialized) + end +1
                return serialized[start:end]
                
            else:
                raise TypeError('list indices must be integers '
                                'or slices, not {}'.format(type(i)))

        def extend(self, iterable):
            if iterable != []:
                current = self._pickler.loads(self._client.get(self._oid))
                current.extend(iterable)
                self._client.set(self._oid, self._pickler.dumps(current))

        def append(self, obj):
            current = self._pickler.loads(self._client.get(self._oid))
            current.append(obj)
            self._client.set(self._oid, self._pickler.dumps(current))

        def pop(self, index=None):
            if index is None:
                current = self._pickler.loads(self._client.get(self._oid))
                item = current.pop()
                self._client.set(self._oid, self._pickler.dumps(current))
                return item
            else:
                item = self[index]
                sentinel = util.get_uuid()
                self[index] = sentinel
                self.remove(sentinel)
                return item

        def __deepcopy__(self, memo):
            selfcopy = type(self)()
            
            memo[id(self)] = selfcopy

            #current = self._pickler.loads(self._client.get(self._oid)
            return selfcopy

        def __add__(self, x):
            # FIXME: list only allows concatenation to other list objects
            #        (altough it can now be extended by iterables)
            # newlist = deepcopy(self)
            # return newlist.__iadd__(x)
            return self[:] + x

        def __iadd__(self, x):
            # FIXME: list only allows concatenation to other list objects
            #        (altough it can now be extended by iterables)
            self.extend(x)
            return self

        def __mul__(self, n):
            if not isinstance(n, int):
                raise TypeError("TypeError: can't multiply sequence"
                                " by non-int of type {}".format(type(n)))
            if n < 1:
                # return type(self)()
                return []
            else:
                # newlist = type(self)()
                # newlist._extend_same_type(self, repeat=n)
                # return newlist
                return self[:] * n

        def __rmul__(self, n):
            return self.__mul__(n)

        def __imul__(self, n):
            if not isinstance(n, int):
                raise TypeError("TypeError: can't multiply sequence"
                                " by non-int of type {}".format(type(n)))
            if n > 1:
                return self[:] * n

        def __len__(self):
            return len(self._pickler.loads(self._client.get(self._oid)))

        def remove(self, obj):
            current = self._pickler.loads(self._client.get(self._oid))
            current.remove(obj)
            self._client.set(self._oid, self._pickler.dumps(current))
            return self

        def __delitem__(self, i):
            sentinel = util.get_uuid()
            self[i] = sentinel
            self.remove(sentinel)

        def tolist(self):
            return self._pickler.loads(self._client.get(self._oid))

        def reverse(self):
            current = self._pickler.loads(self._client.get(self._oid))
            current.reverse()
            self._client.set(self._oid, self._pickler.dumps(current))
            return self

        def sort(self, key=None, reverse=False):
            current = self._pickler.loads(self._client.get(self._oid))
            current.sort()
            self._client.set(self._oid, self._pickler.dumps(current))
            return self

        def index(self, obj, start=0, end=-1):
            return self[:].index(obj, start, end)

        def count(self, obj):
            return self[:].count(obj)

        def insert(self, index, obj):
            current = self._pickler.loads(self._client.get(self._oid))
            current.insert(index, obj)
            self._client.set(self._oid, self._pickler.dumps(current))

    #old
    class DictProxy1(BaseProxy):

        def __init__(self, *args, **kwargs):
            super().__init__('dict')
            self.update( *args, **kwargs)

        def __setitem__(self, k, v):
            current = self._pickler.loads(self._client.get(self._oid))
            current[k]=v
            self._client.set(self._oid, self._pickler.dumps(current))

        def __getitem__(self, k):
            current = self._pickler.loads(self._client.get(self._oid))
            v = current[k]
            if k not in current.keys():
                raise KeyError(k)
            return v

        def __delitem__(self, k):
            current = self._pickler.loads(self._client.get(self._oid))
            res = current[k]
            del current[k]
            self._client.set(self._oid, self._pickler.dumps(current))
            #if res == 0:
            #    raise KeyError(k)

        def __contains__(self, k):
            current = self._pickler.loads(self._client.get(self._oid))
            return current.contains(k)

        def __len__(self):
            return len(self._pickler.loads(self._client.get(self._oid)))

        def __iter__(self):
            return iter(self.keys())

        def get(self, k, default=None):
            try:
                v = self.__getitem__(k)
            except KeyError:
                return default
            else:
                return v

        def pop(self, k, default=None):
            try:
                v = self.__getitem__(k)
            except KeyError:
                return default
            else:
                self.__delitem__(k)
                return v

        def popitem(self):
            try:
                key = self.keys()[0]
                item = (key, self.__getitem__(key))
                self.__delitem__(key)
                return item
            except IndexError:
                raise KeyError('popitem(): dictionary is empty')

        def setdefault(self, k, default=None):
            current = self._pickler.loads(self._client.get(self._oid))
            res = current.setdefault(k,default)
            self._client.set(self._oid, self._pickler.dumps(current))
            return res

        def update(self, *args, **kwargs):
            temp = self._client.get(self._oid)
            if temp == None:
                current = dict( *args, **kwargs)
                self._client.set(self._oid, self._pickler.dumps(current))
            else:
                current = self._pickler.loads(temp)
                if kwargs !={}:
                    current.update(kwargs)
                else:
                    current.update(*args)
                self._client.set(self._oid, self._pickler.dumps(current))

        def keys(self):
            return self._pickler.loads(self._client.get(self._oid)).keys()

        def values(self):
            return self._pickler.loads(self._client.get(self._oid)).values()

        def items(self):
            return self._pickler.loads(self._client.get(self._oid)).items()

        def clear(self):
            current = self._pickler.loads(self._client.get(self._oid))
            current.clear()
            self._client.set(self._oid, self._pickler.dumps(current))

        def copy(self):
            return type(self)(self.items())

        def todict(self):
            return self._pickler.loads(self._client.get(self._oid))

    
    class DictProxy(BaseProxy):

        def __init__(self, **kwargs):
            super().__init__('dict')
            temp = dict(**kwargs)
            keys = ''
            for k,v in temp.items():
                keys = keys+','+self._uuid+json.dumps(k)
                self._client.set(self._uuid+json.dumps(k), self._pickler.dumps(v))

            self._client.set(self._uuid+'keys', keys)

        def __setitem__(self, k, v):
            self._client.set(self._uuid+json.dumps(k), self._pickler.dumps(v))
            keys = self._client.get(self._uuid+'keys')
            keys = keys.decode('utf-8')
            if json.dumps(k) not in keys:
                self._client.append(self._uuid+'keys', ','+self._uuid+json.dumps(k))

        def __getitem__(self, k):
            r = self._client.get(self._uuid+json.dumps(k))
            if r is None:
                return r
            return self._pickler.loads(r)

        def __delitem__(self, k):
            keys = self._client.get(self._uuid+'keys')
            keys = keys.decode('utf-8').replace(','+self._uuid+json.dumps(k),'')
            self._client.set(self._uuid+'keys', keys)
            self._client.delete(self._uuid+json.dumps(k))
            #if res == 0:
            #    raise KeyError(k)

        def __contains__(self, k):
            keys = self._client.get(self._uuid+'keys')
            #keys = keys.decode('utf-8').split(',')
            #temp = self._client.get_many(self._oid)
            return json.dumps(k) in keys.decode('utf-8')

        def __len__(self):
            return len(self._client.get(self._uuid+'keys').split(',')[1:])

        def __iter__(self):
            return iter(self.keys())

        def get(self, k, default=None):
            try:
                v = self.__getitem__(k)
            except KeyError:
                return default
            else:
                return v

        def pop(self, k, default=None):
            try:
                v = self.__getitem__(k)
            except KeyError:
                return default
            else:
                self.__delitem__(k)
                return v

        def popitem(self):
            try:
                key = self.keys()[0]
                item = (key, self.__getitem__(key))
                self.__delitem__(key)
                return item
            except IndexError:
                raise KeyError('popitem(): dictionary is empty')

        def setdefault(self, k, default=None):
            res = self._client.get(self._uuid+json.dumps(k))
            self._client.set(self._uuid+json.dumps(k), self._pickler.dumps(default))
            return res

        def update(self, kwargs):
            temp = dict(**kwargs)
            keys = self._client.get(self._uuid+'keys')
            keys = keys.decode('utf-8')
            for k,v in temp.items():
                keys = keys+','+self._uuid+json.dumps(k)
                self._client.set(self._uuid+json.dumps(k), self._pickler.dumps(v))
            self._client.set(self._uuid+'keys', keys)

        def keys(self):
            ks = self._client.get(self._uuid+'keys').decode('utf-8').replace(self._uuid,'').split(',')[1:]
            return [json.loads(k) for k in ks]

        def values(self):
            keys = self._client.get(self._uuid+'keys')
            keys = keys.decode('utf-8').split(',')[1:]
            temp = self._client.get_many(keys)
            return [self._pickler.loads(v) for v in temp.values()]

        def items(self):
            keys = self._client.get(self._uuid+'keys')
            keys = keys.decode('utf-8').split(',')[1:]
            temp = self._client.get_many(keys)
            return [(k,self._pickler.loads(v)) for k,v in temp.items()]

        def clear(self):
            keys = self._client.get(self._uuid+'keys')
            keys = keys.decode('utf-8').split(',')[1:]
            for k in keys:
                self._client.delete(self._uuid+json.dumps(k))
            self._client.set(self._uuid+'keys', '')

        def copy(self):
            return type(self)(self.items())

        def todict(self):
            return dict(self.items())

    class NamespaceProxy(BaseProxy):
        def __init__(self, **kwargs):
            super().__init__('Namespace')
            DictProxy.update(self, **kwargs)

        def __getattr__(self, k):
            if k[0] == '_':
                return object.__getattribute__(self, k)
            try:
                return DictProxy.__getitem__(self, k)
            except KeyError:
                raise AttributeError(k)

        def __setattr__(self, k, v):
            if k[0] == '_':
                return object.__setattr__(self, k, v)
            DictProxy.__setitem__(self, k, v)

        def __delattr__(self, k):
            if k[0] == '_':
                return object.__delattr__(self, k)
            try:
                return DictProxy.__delitem__(self, k)
            except KeyError:
                raise AttributeError(k)

        def _todict(self):
            return DictProxy.todict(self)


    class ValueProxy(BaseProxy):
        def __init__(self, typecode='Any', value=None, lock=True):
            super().__init__('Value({})'.format(typecode))
            if value is not None:
                self.set(value)

        def get(self):
            return self._pickler.loads(self._client.get(self._oid))

        def set(self, value):
            self._client.set(self._oid, self._pickler.dumps(value))

        value = property(get, set)


    class ArrayProxy(ListProxy):
        def __init__(self, typecode, sequence, lock=True):
            super().__init__(sequence)

    
    #
    # Definition of SyncManager
    #

    class SyncManager(BaseManager):
        """
        Subclass of `BaseManager` which supports a number of shared object types.

        The types registered are those intended for the synchronization
        of threads, plus `dict`, `list` and `Namespace`.

        The `multiprocessing.Manager()` function creates started instances of
        this class.
        """

    SyncManager.register('list', ListProxy)
    SyncManager.register('dict', DictProxy)
    SyncManager.register('dict1', DictProxy1)
    SyncManager.register('Namespace', NamespaceProxy)
    SyncManager.register('Lock', synchronize.Lock)
    SyncManager.register('RLock', synchronize.RLock)
    SyncManager.register('Semaphore', synchronize.Semaphore)
    SyncManager.register('BoundedSemaphore', synchronize.BoundedSemaphore)
    SyncManager.register('Condition', synchronize.Condition)
    SyncManager.register('Event', synchronize.Event)
    SyncManager.register('Barrier', synchronize.Barrier)
    SyncManager.register('Queue', queues.Queue)
    SyncManager.register('SimpleQueue', queues.SimpleQueue)
    SyncManager.register('JoinableQueue', queues.JoinableQueue)
    SyncManager.register('Value', ValueProxy)
    SyncManager.register('Array', ArrayProxy)
    SyncManager.register('Pool', pool.Pool, can_manage=False)



