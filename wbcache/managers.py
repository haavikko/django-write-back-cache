# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
from future import standard_library
standard_library.install_aliases()
from builtins import str
import logging
from django.db.models import Manager
from django.db import connection
from io import StringIO
from util import util
logger = logging.getLogger('dws')

class DmManager(Manager):
    def dm_update_or_create(self, **kwargs):
        """ based on: http://code.djangoproject.com/attachment/ticket/3182/update_or_create.diff

                     Looks up an object with the given kwargs, creating one if necessary.
                     If the object already exists, then its fields are updated with the
                     values passed in the defaults dictionary.
                     Returns a tuple of (object, created), where created is a boolean
                     specifying whether an object was created.

        obj, created = Person.objects.update_or_create(first_name='John', last_name='Lennon',
                                    defaults={'birthday': date(1940, 10, 9)})

        NOTE: in Django 1.11, update_or_create by default:
        * calls select_for_update()
          this is done inside atomic() block, but the lock probably(?) isn't released until
          a "real" commit, not savepoint commit.
        """
        # from ws.dm import db_locking
        obj, created = self.get_or_create(**kwargs)
        if not created:
            defaults = kwargs.pop('defaults', {})
            for k, v in defaults.items():
                setattr(obj, k, v)
            # db_locking.validate_lock_status([obj])
            obj.save()
        return obj, created

    def bulk_insert(self, values, columns):
        '''
        so far postgres only (COPY is pg-only)
        values is defined like:
        [
          (val0a, val0b, val0c),
          (val1a, val1b, val1c),
          ...
        ]
        columns lists the names of database columns corresponding to values.
        '''
        cur = connection.cursor()
        s = StringIO()
        for tup in values:
            s.write('\t'.join([str(t) for t in tup]))
            s.write('\n')
        s.seek(0) # now start reading from start
        # SLOW logger.debug('bulk_insert data %s', s.getvalue().replace('\n', '<NL>'))
        table_name = self.model._meta.db_table
        cur.copy_from(s, table_name, columns=columns)

    @util.log_execution
    def get_by_natural_key(self, *args):
        '''
        I like to define get_by_natural_key in class, not in manager.
        By default use uuid.
        '''
        logger.debug('get_by_natural_key from %s', self.model)
        if hasattr(self.model, 'get_by_natural_key'):
            return self.model.get_by_natural_key(*args)
        has_uuid = False
        try:
            has_uuid = self.model._meta.get_field('uuid')
        except:
            pass
        if has_uuid:
            util.validate_equals(1, len(args), 'ERROR 394203234: assumption violated')
            return self.get(uuid=args[0])
        raise Exception('default implementation of get_by_natural_key %s not supported by %s', args, self.model)
        #if not len(self.model._natural_key_fields) != len(args) or len(self.model._natural_key_fields) == 0:
        #    raise Exception('%s has invalid _natural_key_fields')
        #kw = dict(zip(self.model._natural_key_fields, args))
        #return self.get(**kw)
