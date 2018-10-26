#!/usr/bin/env python
# -*- coding: utf8 -*-
from __future__ import unicode_literals, absolute_import, print_function

from future.utils import python_2_unicode_compatible, raise_with_traceback
# overloaded by 'future' in python 2.7
from builtins import str

import re

from lxml import etree

from .errors import FmError, XmlError
from .data import MutableDict

from .caster import (
    DummyCast,
    default_cast_map,
    FM_NUMBER,
    FM_TEXT,
    FM_DATE,
    FM_TIME,
    FM_TIMESTAMP,
)

@python_2_unicode_compatible
class FmMeta(object):
    """
    FmMeta objects hold all the information about an xml query to an FMS server
    - date, time and timestamp formats
    - fields definition and how to cast them into python objects
    - relatedset fields definition

    """
    def __init__( self, cast_map=None, server_timezone=None ):
        self.cast_map = default_cast_map
        if cast_map:
            self.cast_map.update( cast_map )
        self.server_timezone = server_timezone

        self.ns = None
        self._ns_length = 0
        self.database = None
        self.error = None
        self.date_pattern = None
        self.time_pattern = None
        self.timestamp_pattern = None
        self.db_name = None
        self.layout = None
        self.table = None
        self.encoding = 'utf8'
        self.fields = {}

    def get_tagname( self, node ):
        """Returns the tag name striped from the ns name at the begining"""
        return node.tag[self._ns_length:]

    def add_field( self, field ):
        """Adds an FmFieldBase object in the current context"""
        # name = field.name
        raw_name = field.raw_name
        if not raw_name:
            return

        field.splitted_names = raw_name.split('::',1)
        if field.table and field.table != field.splitted_names[0]:
            field.splitted_names = tuple( [field.table] + field.splitted_names )

        ukey = raw_name
        if field.table:
            ukey = "{}__{}".format(field.table, raw_name)
        self.fields[ukey] = field

    def get_fm_field( self, raw_name, table ):
        if not table:
            return self.fields[raw_name]
        return self.fields[ "{}__{}".format(table, raw_name) ]

    def set_context( self, name, field=None ):
        if name is None or name == '':
            self._context = self.fields
            return
        if name not in self.fields:
            self.fields[name] = field
        self._context = self.fields[name]

    def decode_attrs( self, value ):
        if not isinstance( value, str ):
            return value.decode(self.encoding)
        return value

    def decode_data( self, value ):
        if value is not None:
            if not isinstance( value, str ):
                return value.decode(self.encoding)
            return value
        return None

    # def sanitize_field_identifier( self, value ):
    #     val = unidecode(value)
    #     return val.replace('::','_')

    def parse_fm_dates_formats( self, formats ):
        """
        Takes a dict build from the datasource node of an xml FMS result, e.g.
        <datasource 
        ...
        date-format="MM/dd/yyyy" 
        time-format="HH:mm:ss" 
        timestamp-format="MM/dd/yyyy HH:mm:ss">
        </datasource>

        And converts all the datetime formats to formats that can be used in 
        datetime.datetime.strptime
        """
        def _build_one( stamp ):
            return stamp.\
              replace( 'yyyy', '%Y' ).\
              replace( 'MM', '%m' ).\
              replace( 'dd', '%d' ).\
              replace( 'HH', '%H' ).\
              replace( 'mm', '%M' ).\
              replace( 'ss', '%S' )

        return {
            'date': _build_one( formats.get('date-format', '') ),
            'timestamp': _build_one( formats.get('timestamp-format', '') ),
            'time': _build_one( formats.get('time-format', '') ),
        }


    def get_record_class( self ):
        return MutableDict

    def __repr__( self ):
        return "<{klass} ({x.db_name},layout:{x.layout}) {error} fields:({fields})>".format(
            klass=self.__class__.__name__,
            x=self,
            error = self.error if self.error else '',
            fields = ", ".join( self.fields.keys() ),
        )

    def dump( self ):
        """Verbose representation of the object"""
        s = [ "  " + field.dump() for field in self.fields.values() ]
        return "{klass}( ({x.db_name},layout:{x.layout}) {error} ):\n{fields}".format(
            klass=self.__class__.__name__,
            x=self,
            error = self.error if self.error else '',
            fields = "\n".join( s )
        )

class FmFieldBase(object):
    def __init__( self, raw_name, attrs=None, fm_meta=None, table=None ):
        self.raw_name = raw_name
        self.attrs = attrs
        self.fm_meta = fm_meta
        self.table = table
        self.is_multi = 1 # means False. >1 is True.

    def set_value( self, record, data_list ):
        pass

    def dump(self):
        """Verbose representation of the object"""
        return "'{}'".format( self.raw_name )

class FmFieldData(FmFieldBase):
    def __init__( self, raw_name, attrs=None, fm_meta=None, table=None ):
        super( FmFieldData, self ).__init__(raw_name=raw_name,attrs=attrs,fm_meta=fm_meta,table=table)
        self.is_multi = 0
        if fm_meta:
            result = attrs.get('result', None)
            caster_class = fm_meta.cast_map.get( result, DummyCast )
            self.caster = caster_class(
                fm_field=self,
                fm_meta=fm_meta,
            )
            self.is_multi = int( attrs.get('max-repeat','1') )

    def set_value( self, record, data_list ):
        value = data_list
        if self.is_multi == 1:
            value = data_list[0]

        if len(self.splitted_names) == 1:
            record[self.splitted_names[0]] = value
        else:
            ctxs = self.splitted_names[:-1]
            if ctxs[0] == self.table:
                ctxs = ctxs[1:]
            name = self.splitted_names[-1]
            obj = record
            for ctx in ctxs:
                try:
                    obj = obj[ctx]
                except KeyError:
                    obj[ctx] = self.fm_meta.get_record_class()()
            obj[ name ] = value


    def dump(self):
        """Verbose representation of the object"""
        return "{}: type:{}".format( self.raw_name, self.caster.__class__.__name__ )

class FmFieldContainer(FmFieldBase, dict):
    def __init__( self, raw_name, attrs=None, fm_meta=None ):
        super( FmFieldContainer, self ).__init__(raw_name=raw_name,attrs=attrs,fm_meta=fm_meta)
    
    def dump(self):
        """Verbose representation of the object"""
        s = [ "     " + field.dump() for field in self.values() ]
        return "Container({})\n{}".format(
            self.raw_name,
            "\n".join(s),
        )

def metadata_parser( node, fm_meta, table=None ):
    """
    Parses the <metadata> node from the xml.
    :fm_meta: is the FmMeta object that is to be defined
    :node: is either the <metadata> node or a <relatedset-definition> subnode
    :table: is defined only when in a <realtedset-definition> node.
    """
    tag = fm_meta.get_tagname( node )
    if tag == 'metadata':
        for subnode in node:
            metadata_parser( 
                node=subnode, 
                fm_meta=fm_meta,
                table=table,
            )
    elif tag == 'field-definition':
        f = fm_meta.decode_attrs
        attrs = { f(k):f(v) for k,v in node.attrib.items() }
        raw_name = attrs['name']
        f = FmFieldData( raw_name=raw_name, attrs=attrs, fm_meta=fm_meta, table=table )
        fm_meta.add_field( f )

    elif tag == 'relatedset-definition':
        f = fm_meta.decode_attrs
        attrs = { f(k):f(v) for k,v in node.attrib.items() }
        table = attrs['table']

        f = FmFieldContainer( raw_name=table, fm_meta=fm_meta )
        try:
            for subnode in node:
                metadata_parser(
                    node=subnode,
                    fm_meta=fm_meta,
                    table=table,
                )
        except Exception as e:
            # reraise, i.e. raise e.with_traceback()
            raise_with_traceback(e)
        finally:
            table = None

