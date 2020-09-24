import sortedcontainers
import collections
import logging
import operator

from util import util

logger = logging.getLogger('dws')

import threading


class Sequence:
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()

    def next_value(self):
        with self._lock:
            self.value += 1
            ret = self.value
        return ret


'''

Usage pattern:
* Read all/most of the relevant data in bulk
* Perform calculations in-memory
* Write all changes at the end



Aliasing:
* Do not modify the index fields directly

Stale data in memory:
* Use row level locking in database or some other concurrency mechanism

Signals:
* Adding/deletingo in-memory store does not invoke signals

'''


class LookupException(Exception):
    pass

class NotFound(LookupException):
    pass

class LookupContainer:
    LOOKUP_NOT_SUPPORTED = 0  # can not do lookup with parameters given
    VALUE_UNKNOWN = 1  # key is outside scope of the cache (the data might still exist in some backend data store)
    VALUE_DOES_NOT_EXIST = 2  # it is known that the value for this key does not exist (in the backend data store, for example)
    VALUE_FOUND = 3  # result was found from cache

    def add(self, elem):
        pass

    def delete(self, elem):
        pass

    def clear(self):
        pass

    def bulk_add(self, items):
        pass

    def can_answer_authoritatively(self, **parameters):
        # return True if missing value in cache means that the
        # value does not exist at all
        return False

    def lookup(self, **parameters):
        util.fail('abstract')

    def first(self, **parameters):
        retcode, value = self.lookup(**parameters)
        if retcode == self.VALUE_FOUND:
            return retcode, value[0]
        else:
            return retcode, None

    def get(self, **parameters):
        retcode, value = self.lookup(**parameters)
        if retcode == self.VALUE_FOUND:
            if len(value) != 1:
                raise LookupException('get() found %s objects instead of 1', len(value))
            return value[0]
        else:
            return None


class DjangoFilterLookup(LookupContainer):
    '''
    DjangoFilterLookup is meant to be used as a backup solution, when
    value is not found from in-memory LookupContainers.
    DjangoFilterLookup does not implement add() or delete(),
    because these should be handled via ChangeLog of InProcessWriteBackCache.

    Usually it is desirable to restrict the queries made through
    this interface, so that the caller does not get inconsistent results.
    '''

    def __init__(self, base_queryset, restrict_query=None):
        self.base_queryset = base_queryset
        self.restrict_query = restrict_query

    '''
    def first(self, **parameters):
        if self.restrict_query and self.restrict_query(**parameters):
            return self.LOOKUP_NOT_SUPPORTED, None
        value = self._do_filter(**parameters).first()
        if value:
            return self.VALUE_FOUND, value
        else:
            return self.VALUE_DOES_NOT_EXIST, None
    '''

    def lookup(self, **parameters):
        if self.restrict_query and self.restrict_query(**parameters):
            return self.LOOKUP_NOT_SUPPORTED, None
        qs = self._do_filter(**parameters)
        if qs:
            return self.VALUE_FOUND, qs
        else:
            return self.VALUE_DOES_NOT_EXIST, qs

    def _do_filter(self, **parameters):
        return self.base_queryset.filter(**parameters)

    def can_answer_authoritatively(self, **parameters):
        # because DjangoFilterLookup
        True


class DjangoReadWriteInterface(DjangoFilterLookup):
    '''
    LookupContainer that does no caching, passes all requests to underlying model/queryset
    '''

    def add(self, elem):
        elem.save()

    def delete(self, elem):
        elem.delete()

    def clear(self):
        pass


class ListLookup(LookupContainer):
    '''
    Keep objects in a dict of sorted lists, for faster retrieval
    For each key, cache a list of objects
    '''

    def __init__(self, key_attributes, sort_key=None):
        self.key_attributes = key_attributes
        self._index = collections.defaultdict(lambda: sortedcontainers.SortedSet(key=sort_key))

    def _key(self, elem):
        return tuple([getattr(elem, k) for k in self.key_attributes])

    def bulk_add(self, items):
        for item in items:
            key = self._key(item)
            self._index[key].add(item)

    def add(self, elem):
        '''
        Add element to container.
        Note: once added, the hash and sort order of the element must not change

        We don't want duplicate items in the index.
        Duplicates are determined using object equality.
        When a element is added, duplicates are removed.

        How Django model instance equality works (checked Django 3.1)
        * if model instance has pk set to None, then it is not equal to any other object,
          even if all their fields (including pk) are equal.
        * if both objects have pk set, then equality is determined by comparing pk value

        Note that if model instance pk value is None, then the object is not hashable.
        '''
        key = self._key(elem)
        self._index[key].discard(elem)  # e.g. existing Django model instance with same pk
        self._index[key].add(elem)

    def clear(self):
        self._index.clear()

    def delete(self, elem):
        key = self._key(elem)
        util.validate(key in self._index, 'ERROR 934923432')
        self._index[key].remove(elem)

    def __len__(self):
        return len(self._index)

    def lookup(self, **parameters):
        if len(parameters) != len(self.key_attributes):
            return self.LOOKUP_NOT_SUPPORTED, None
        try:
            key = tuple([parameters[k] for k in self.key_attributes])
        except KeyError:
            return self.LOOKUP_NOT_SUPPORTED, None

        if key in self._index and self._index[key]:
            return self.VALUE_FOUND, self._index[key]
        else:
            if self.can_answer_authoritatively(**parameters):
                return self.VALUE_DOES_NOT_EXIST, None
            else:
                return self.VALUE_UNKNOWN, None
        util.fail('unreachable')

    def __str__(self):
        return 'ListLookup %s' % self.key_attributes

    def __repr__(self):
        return 'ListLookup %s' % self.key_attributes



class DefaultListLookup(LookupContainer):
    '''
    Keep objects in a dict of sorted lists, for faster retrieval
    For each key, cache a list of objects
    '''

    def __init__(self, key_attributes, sort_key=None):
        self.key_attributes = key_attributes
        self._sort_key = sort_key
        self._index = collections.defaultdict(list)

    def _key(self, elem):
        return tuple([getattr(elem, k) for k in self.key_attributes])

    def bulk_add(self, items):
        if len(self._index) == 0:
            for item in items:
                key = self._key(item)
                self._index[key].append(item)
            for val in self._index.values():
                val.sort(key=self._sort_key)
        else:
            for item in items:
                self.add(item)

    def add(self, elem):
        '''
        Add element to container.

        We don't want duplicate items in the index.
        Duplicates are determined using object equality.
        When a element is added, duplicates are removed.

        How Django model instance equality works (checked Django 3.1)
        * if model instance has pk set to None, then it is not equal to any other object,
          even if all their fields (including pk) are equal.
        * if both objects have pk set, then equality is determined by comparing pk value

        Note that if model instance pk value is None, then the object is not hashable.
        '''
        key = self._key(elem)
        try:
            self._index[key].remove(elem)
        except ValueError:
            pass
        self._index[key].append(elem)
        '''
        try:
            idx = self._index[key].index(elem)  # earlier duplicate?
            self._index[key][idx] = elem
        except ValueError:
            self._index[key].append(elem)
        '''
        self._index[key].sort(key=self._sort_key)

    def clear(self):
        self._index.clear()

    def delete(self, elem):
        key = self._key(elem)
        util.validate(key in self._index, 'ERROR 934923432')
        self._index[key] = [e for e in self._index[key] if e != elem]

    def __len__(self):
        return len(self._index)

    def lookup(self, **parameters):
        if len(parameters) != len(self.key_attributes):
            return self.LOOKUP_NOT_SUPPORTED, None
        try:
            key = tuple([parameters[k] for k in self.key_attributes])
        except KeyError:
            return self.LOOKUP_NOT_SUPPORTED, None

        if self._index[key]:
            return self.VALUE_FOUND, self._index[key]
        else:
            if self.can_answer_authoritatively(**parameters):
                return self.VALUE_DOES_NOT_EXIST, None
            else:
                return self.VALUE_UNKNOWN, None
        util.fail('unreachable')

    def __str__(self):
        return 'ListLookup %s' % self.key_attributes

    def __repr__(self):
        return 'ListLookup %s' % self.key_attributes


'''
class UnknownPrimaryKey:
    key_counter = Sequence()

    def __init__(self):
        self.unique_counter = UnknownPrimaryKey.key_counter.increment()

    def __eq__(self, other):
        return isinstance(other, UnknownPrimaryKey and other.unique_counter == self.unique_counter)
'''


class UniqueTransientValue:
    # A hashable value that can be compared to anything and is only equal to itself.
    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return super().__hash__()  # use default implementation based on id()


class DjangoModelLookup(ListLookup):

    def add(self, elem):
        if elem.pk is None:
            elem.pk = UniqueTransientValue()
        super().add(elem)

    def delete(self, elem):
        if elem.pk is None:
            util.fail('ERROR 3942342: attempting to delete object that was never added')
            return
        else:
            super().delete(elem)

class CompositeLookup(LookupContainer):
    '''
    in-memory cacheable data
    '''

    def __init__(self, indexes=None):
        super().__init__()
        self.indexes = []
        self.clear()
        if indexes:
            for idx in indexes:
                self._add_index(idx)

    def clear(self):
        for idx in self.indexes:
            idx.clear()

    def _add_index(self, index):
        self.indexes.append(index)

    def add(self, elem):
        for idx in self.indexes:
            idx.add(elem)

    def bulk_add(self, items):
        for idx in self.indexes:
            idx.bulk_add(items)

    def delete(self, elem):
        for idx in self.indexes:
            idx.delete(elem)

    def lookup(self, **parameters):
        retcode = self.LOOKUP_NOT_SUPPORTED
        for idx in self.indexes:
            new_retcode, result = idx.lookup(**parameters)
            if new_retcode in [self.VALUE_FOUND, self.VALUE_DOES_NOT_EXIST]:
                # got a certain result
                return new_retcode, result
            else:
                retcode = max(retcode, new_retcode)
        return retcode, None

    '''
    Note : don't need to implement get(), default implementation is fine as it uses lookup()
    def get(self, **parameters):
        for idx in self.indexes:
            value = idx.get(**parameters)
            if value is not None:
                return value
        return None
    '''


class DjangoModelChange:
    _change_counter = Sequence()

    def __init__(self, obj):
        self.obj = obj
        self.change_counter = DjangoModelChange._change_counter.next_value()

    @property
    def pk(self):
        return self.obj.pk


class DeleteOp(DjangoModelChange):
    def __init__(self, obj):
        super().__init__(obj)
        self.change_type = 'delete'

    def apply_change(self):
        if self.obj.pk is None:
            # object was created and then deleted before it was ever written to database
            pass
        else:
            self.obj.save()


class SaveOp(DjangoModelChange):
    def __init__(self, obj, **save_kwargs):
        super().__init__(obj)
        self.change_type = 'save'
        self.save_kwargs = save_kwargs

    def apply_change(self):
        if isinstance(self.obj.pk, UniqueTransientValue):
            # real pk value will be assigned on save()
            self.obj.pk = None
        self.obj.save(**self.save_kwargs)


class ChangeLog(ListLookup):
    '''
    Specialized container

    Handling of created objects:
    '''

    def __init__(self):
        super().__init__(key_attributes=['pk'], sort_key=operator.attrgetter('change_counter'))

    def add(self, change):
        util.validate(isinstance(change, DjangoModelChange))
        super().add(change)

    def delete(self, change):
        util.fail('ERROR 403242323: Entries can not be deleted from change log - instead add a DeleteOp()')

    def apply_all(self):
        # only apply the _last_ state of each object.
        # apply changes in the order they were made, to preserve constraints
        latest_changes_for_each_pk_value = []
        for pk, changes in self._index.items():
            if not changes:
                continue
            latest_changes_for_each_pk_value.append(changes[-1])
        latest_changes_for_each_pk_value.sort(key=operator.attrgetter('change_counter'))
        for change in latest_changes_for_each_pk_value:
            change.apply_change()


class InProcessWriteBackCache(CompositeLookup):
    '''
    in-process write back cache for Django Model instances.
    NOTE: newly created objects do not get a real primary key value until saved to database.
    If adding a model instance with pk set to None, then a temporary UniqueValue object
    is assigned as pk (to make the pk hashable)
    '''

    def __init__(self, indexes):
        # self.pk_index = ListLookup(['pk'], sort_key=operator.attrgetter('pk'))
        if indexes is None:
            indexes = []
        # indexes.append(self.pk_index)
        self.change_log = ChangeLog()
        super().__init__(indexes)

    def is_cacheable(self, elem):
        util.fail('abstract')

    def is_cached(self, object_id):
        return object_id in self.in_memory_object_ids

    def add(self, elem):
        op = SaveOp(elem)
        if self.is_cacheable(elem):
            if elem.pk is None:
                elem.pk = UniqueTransientValue()
            super().add(elem)
            self.change_log.add(op)
        else:
            op.apply_change()

    def delete(self, elem):
        op = DeleteOp(elem)
        if self.is_cacheable(elem):
            super().delete(elem)
            self.change_log.add(op)
        else:
            op.apply_change()

    def flush_changes_to_database(self):
        self.change_log.apply_all()
        self.change_log.clear()

    def clear(self, force=False):
        super().clear()
        if not force:
            util.validate_equals(0, len(self.change_log))
        self.change_log.clear()

    def load_to_memory(self, object_ids, add_to_existing_cache=False):
        util.fail('abstract')
