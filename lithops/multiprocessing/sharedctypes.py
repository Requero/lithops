#
# Module which supports allocation of ctypes objects from shared memory
#
# multiprocessing/sharedctypes.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Cloudlab URV
#

import ctypes
import cloudpickle
import logging

from . import util
from . import get_context
from . import config as mp_config

logger = logging.getLogger(__name__)

typecode_to_type = {
    'c': ctypes.c_char, 'u': ctypes.c_wchar,
    'b': ctypes.c_byte, 'B': ctypes.c_ubyte,
    'h': ctypes.c_short, 'H': ctypes.c_ushort,
    'i': ctypes.c_int, 'I': ctypes.c_uint,
    'l': ctypes.c_long, 'L': ctypes.c_ulong,
    'q': ctypes.c_longlong, 'Q': ctypes.c_ulonglong,
    'f': ctypes.c_float, 'd': ctypes.c_double
}


if 'redis' in util. mp_config.get_parameter(mp_config.CACHE) :

    class SharedCTypeProxy:
        def __init__(self, ctype, *args, **kwargs):
            self._typeid = ctype.__name__
            self._oid = '{}-{}'.format(self._typeid, util.get_uuid())
            #self._client = util.get_redis_client()
            self._client = util.get_cache_client()
            self._ref = util.RemoteReference(self._oid, client=self._client)
            logger.debug('Requested creation on shared C type %s', self._oid)


    class RawValueProxy(SharedCTypeProxy):
        def __init__(self, ctype, *args, **kwargs):
            super().__init__(ctype=ctype)

        def __setattr__(self, key, value):
            if key == 'value':
                obj = cloudpickle.dumps(value)
                logger.debug('Set raw value %s of size %i B', self._oid, len(obj))
                self._client.set(self._oid, obj, ex=mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            else:
                super().__setattr__(key, value)

        def __getattr__(self, item):
            if item == 'value':
                obj = self._client.get(self._oid)
                if not obj:
                    logger.debug('Get value %s returned None', self._oid)
                    value = 0
                else:
                    logger.debug('Get value %s of size %i B', self._oid, len(obj))
                    value = cloudpickle.loads(obj)
                return value
            else:
                super().__getattribute__(item)


    class RawArrayProxy(SharedCTypeProxy):
        def __init__(self, ctype, *args, **kwargs):
            super().__init__(ctype)

        def _append(self, value):
            obj = cloudpickle.dumps(value)
            self._client.rpush(self._oid, obj)

        def __len__(self):
            return self._client.llen(self._oid)

        def __getitem__(self, i):
            if isinstance(i, slice):
                start, stop, step = i.indices(self.__len__())
                logger.debug('Requested get list slice from %i to %i', start, stop)
                objl = self._client.lrange(self._oid, start, stop)
                self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                return [cloudpickle.loads(obj) for obj in objl]
            else:
                obj = self._client.lindex(self._oid, i)
                logger.debug('Requested get list index %i of size %i B', i, len(obj))
                return cloudpickle.loads(obj)

        def __setitem__(self, i, value):
            if isinstance(i, slice):
                start, stop, step = i.indices(self.__len__())
                for i, val in enumerate(value):
                    self[i + start] = val
            else:
                obj = cloudpickle.dumps(value)
                logger.debug('Requested set list index %i of size %i B', i, len(obj))
                self._client.lset(self._oid, i, obj)
                self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))


    class SynchronizedSharedCTypeProxy(SharedCTypeProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype=ctype)
            if lock:
                self._lock = lock
            else:
                ctx = ctx or get_context()
                self._lock = ctx.RLock()
            self.acquire = self._lock.acquire
            self.release = self._lock.release

        def __enter__(self):
            return self._lock.__enter__()

        def __exit__(self, *args):
            return self._lock.__exit__(*args)

        def get_obj(self):
            raise NotImplementedError()

        def get_lock(self):
            return self._lock


    class SynchronizedValueProxy(RawValueProxy, SynchronizedSharedCTypeProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype=ctype, lock=lock, ctx=ctx)

        def get_obj(self):
            return self.value


    class SynchronizedArrayProxy(RawArrayProxy, SynchronizedSharedCTypeProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype=ctype, lock=lock, ctx=ctx)

        def get_obj(self):
            return self[:]


    class SynchronizedStringProxy(SynchronizedArrayProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype, lock=lock, ctx=ctx)

        def __setattr__(self, key, value):
            if key == 'value':
                for i, elem in enumerate(value):
                    obj = cloudpickle.dumps(elem)
                    logger.debug('Requested set string index %i of size %i B', i, len(obj))
                    self._client.lset(self._oid, i, obj)
                    self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
            else:
                super().__setattr__(key, value)

        def __getattr__(self, item):
            if item == 'value':
                return self[:]
            else:
                super().__getattribute__(item)

        def __getitem__(self, i):
            if isinstance(i, slice):
                start, stop, step = i.indices(self.__len__())
                logger.debug('Requested get string slice from %i to %i', start, stop)
                objl = self._client.lrange(self._oid, start, stop)
                self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                return bytes([cloudpickle.loads(obj) for obj in objl])
            else:
                obj = self._client.lindex(self._oid, i)
                self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                return bytes([cloudpickle.loads(obj)])

elif 'memcached' in util. mp_config.get_parameter(mp_config.CACHE) :

    class SharedCTypeProxy:
        def __init__(self, ctype, *args, **kwargs):
            self._typeid = ctype.__name__
            self._oid = '{}-{}'.format(self._typeid, util.get_uuid())
            #self._client = util.get_redis_client()
            self._client = util.get_cache_client()
            self._ref = util.RemoteReference(self._oid, client=self._client)
            logger.debug('Requested creation on shared C type %s', self._oid)


    class RawValueProxy(SharedCTypeProxy):
        def __init__(self, ctype, *args, **kwargs):
            super().__init__(ctype=ctype)

        def __setattr__(self, key, value):
            if key == 'value':
                obj = cloudpickle.dumps(value)
                logger.debug('Set raw value %s of size %i B', self._oid, len(obj))
                self._client.set(self._oid, obj)
            else:
                super().__setattr__(key, value)

        def __getattr__(self, item):
            if item == 'value':
                obj = self._client.get(self._oid)
                if not obj:
                    logger.debug('Get value %s returned None', self._oid)
                    value = 0
                else:
                    logger.debug('Get value %s of size %i B', self._oid, len(obj))
                    value = cloudpickle.loads(obj)
                return value
            else:
                super().__getattribute__(item)
    

    class RawArrayProxy(SharedCTypeProxy):
        def __init__(self, ctype, *args, **kwargs):
            super().__init__(ctype)
            self._client.set(self._oid+'count',0)

        def _append(self, value):
            count = int(self._client.incr(self._oid+'count',1))
            self._client.set(str(self._oid)+str(count-1), cloudpickle.dumps(value))

        def __len__(self):
            return int(self._client.get(self._oid+'count'))

        def __getitem__(self, i):
            if isinstance(i, slice):
                start, stop, step = i.indices(self.__len__())
                keys = [str(self._oid)+str(j) for j in range(start, stop, step)]
                logger.debug('Requested get list slice from %i to %i', start, stop)
                objs = self._client.get_many(keys)
                return [cloudpickle.loads(obj) for obj in objs.values()]
            else:
                obj = self._client.get(str(self._oid)+str(i))
                logger.debug('Requested get list index %i of size %i B', i, len(obj))
                return cloudpickle.loads(obj)

        def __setitem__(self, i, value):
            if isinstance(i, slice):
                start, stop, step = i.indices(self.__len__())
                for i, val in enumerate(value):
                    self[i + start] = val
            else:
                obj = cloudpickle.dumps(value)
                logger.debug('Requested set list index %i of size %i B', i, len(obj))
                self._client.set(str(self._oid)+str(i), obj)


    class SynchronizedSharedCTypeProxy(SharedCTypeProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype=ctype)
            if lock:
                self._lock = lock
            else:
                ctx = ctx or get_context()
                self._lock = ctx.RLock()
            self.acquire = self._lock.acquire
            self.release = self._lock.release

        def __enter__(self):
            return self._lock.__enter__()

        def __exit__(self, *args):
            return self._lock.__exit__(*args)

        def get_obj(self):
            raise NotImplementedError()

        def get_lock(self):
            return self._lock


    class SynchronizedValueProxy(RawValueProxy, SynchronizedSharedCTypeProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype=ctype, lock=lock, ctx=ctx)

        def get_obj(self):
            return self.value


    class SynchronizedArrayProxy(RawArrayProxy, SynchronizedSharedCTypeProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype=ctype, lock=lock, ctx=ctx)

        def get_obj(self):
            return self[:]


    class SynchronizedStringProxy(SynchronizedArrayProxy):
        def __init__(self, ctype, lock=None, ctx=None, *args, **kwargs):
            super().__init__(ctype, lock=lock, ctx=ctx)

        def __setattr__(self, key, value):
            if key == 'value':
                #current = self._pickler.loads(self._client.get(self._oid))
                for i, elem in enumerate(value):
                    #obj = cloudpickle.dumps(elem)
                    logger.debug('Requested set string index %i of size %i B', i, len(obj))
                    #current.insert(i, elem)
                    self._client.set(self._oid+str(i), cloudpickle.dumps(elem))
            else:
                super().__setattr__(key, value)

        def __getattr__(self, item):
            if item == 'value':
                return self[:]
            else:
                super().__getattribute__(item)

        def __getitem__(self, i):
            if isinstance(i, slice):
                start, stop, step = i.indices(self.__len__())
                keys = [str(self._oid)+str(j) for j in range(start, stop, step)]
                logger.debug('Requested get string slice from %i to %i', start, stop)
                objl = self._client.get_many(keys)
                #objl = self._client.lrange(self._oid, start, stop)
                #self._client.expire(self._oid, mp_config.get_parameter(mp_config.REDIS_EXPIRY_TIME))
                #self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                #objl = self._client.get_many(keys)
                #print(self._client.get(self._oid))
                #print(cloudpickle.loads(self._client.get(self._oid)))
                #return cloudpickle.loads(self._client.get(self._oid))[start:stop]
                return bytes([cloudpickle.loads(obj) for obj in objl.values()])
            else:
                #obj = self._client.lindex(self._oid, i)
                #self._client.expire(self._oid, mp_config.get_parameter(mp_config.REDIS_EXPIRY_TIME))
                #self._client.expire(self._oid, mp_config.get_parameter(mp_config.CACHE_EXPIRY_TIME))
                return bytes([cloudpickle.loads(self._client.get(self._oid+str(i)))])
                #return cloudpickle.loads(self._client.get(self._oid+str(i)))


#
#
#


def RawValue(typecode_or_type, initial_value=None):
    """
    Returns a ctypes object allocated from shared memory
    """
    logger.debug('Requested creation of resource RawValue')
    type_ = typecode_to_type.get(typecode_or_type, typecode_or_type)
    obj = RawValueProxy(type_)
    if initial_value:
        obj.value = initial_value
    return obj


def RawArray(typecode_or_type, size_or_initializer):
    """
    Returns a ctypes array allocated from shared memory
    """
    logger.debug('Requested creation of resource RawArray')
    type_ = typecode_to_type.get(typecode_or_type, typecode_or_type)
    if type_ is ctypes.c_char:
        raise NotImplementedError()
    else:
        obj = RawArrayProxy(type_)

    if isinstance(size_or_initializer, list):
        for elem in size_or_initializer:
            obj._append(elem)
    elif isinstance(size_or_initializer, int):
        for _ in range(size_or_initializer):
            obj._append(0)
    else:
        raise ValueError('Invalid size or initializer {}'.format(size_or_initializer))

    return obj


def Value(typecode_or_type, initial_value=None, lock=True, ctx=None):
    """
    Return a synchronization wrapper for a Value
    """
    logger.debug('Requested creation of resource Value')
    type_ = typecode_to_type.get(typecode_or_type, typecode_or_type)
    obj = SynchronizedValueProxy(type_)
    if initial_value is not None:
        obj.value = initial_value
    return obj


def Array(typecode_or_type, size_or_initializer, *, lock=True, ctx=None):
    """
    Return a synchronization wrapper for a RawArray
    """
    logger.debug('Requested creation of resource Array')
    type_ = typecode_to_type.get(typecode_or_type, typecode_or_type)
    if type_ is ctypes.c_char:
        obj = SynchronizedStringProxy(type_)
    else:
        obj = SynchronizedArrayProxy(type_)

    if isinstance(size_or_initializer, list) or isinstance(size_or_initializer, bytes):
        for elem in size_or_initializer:
            obj._append(elem)
    elif isinstance(size_or_initializer, int):
        for _ in range(size_or_initializer):
            obj._append(0)
    else:
        raise ValueError('Invalid size or initializer {}'.format(size_or_initializer))

    return obj
