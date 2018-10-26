# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import, print_function

import datetime

from builtins import str

FM_NUMBER = 'number'
FM_TEXT = 'text'
FM_DATE = 'date'
FM_TIME = 'time'
FM_TIMESTAMP = 'timestamp'

class TypeCast(object):
    """Type caster, get's initiated with the corresponding FmFieldData"""
    def __init__( self, fm_field, fm_meta ):
        pass

    def __call__( self, value ):
        return value

class NumberCast(TypeCast):
    def __call__( self, value ):
        try:
            return float( value )
        except Exception:
            # return NaN
            return float('nan')

class TextCast(TypeCast):
    def __call__( self, value ):
        if value:
            return value
        return ''

DummyCast=TextCast

class DateCast(TypeCast):
    def __init__( self, fm_field, fm_meta ):
        self.pat = fm_meta.date_pattern

    def __call__( self, value ):
        try:
            d = datetime.datetime.strptime(
                value,
                self.pat,
            )
            return d.date()
        except (ValueError,TypeError):
            return None

class TimeCast(TypeCast):
    def __init__( self, fm_field, fm_meta ):
        self.pat = fm_meta.time_pattern
    def __call__( self, value ):
        try:
            return datetime.datetime.strptime(
                value,
                self.pat,
            ).time()
        except (ValueError,TypeError):
            return None

class TimestampCast(TypeCast):
    def __init__( self, fm_field, fm_meta ):
        self.pat = fm_meta.timestamp_pattern
        self.tz = fm_meta.server_timezone
    def __call__( self, value ):
        try:
            d = datetime.datetime.strptime(
                value,
                self.pat,
            )
            if self.tz:
                d = self.tz.localize( d )
                d = self.tz.normalize( d )
            return d
        except (ValueError,TypeError):
            return None

class BackCast(object):
    """Cast from python to xml in do_edit or do_new or find arguments"""
    FM_DEFAULT_DATE = "%m/%d/%Y"
    FM_DEFAULT_TIME = "%H:%M:%S"
    FM_DEFAULT_TIMESTAMP = "%m/%d/%Y %H:%M:%S"

    def __init__( self, fm_server=None ):
        """The :fm_server: object is passed at the initialisation of this class.
        It can be used to cast some field in a different way"""
        if fm_server:
            self.tz = fm_server.options['server_timezone']

    def __call__( self, field, value ):
        if isinstance( value, datetime.datetime ):
            # if server timezone is set and the datetime is aware:
            if self.tz and value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None:
                if self.tz != value.tzinfo:
                    value = value.astimezone(self.tz)
            return value.strftime( self.__class__.FM_DEFAULT_TIMESTAMP )

        elif isinstance( value, datetime.date ):
            return value.strftime( self.__class__.FM_DEFAULT_DATE )
        elif isinstance( value, datetime.time ):
            return value.strftime( self.__class__.FM_DEFAULT_TIME )
        return str(value)

default_cast_map = {
    FM_NUMBER : NumberCast,
    FM_TEXT : TextCast,
    FM_DATE : DateCast,
    FM_TIME : TimeCast,
    FM_TIMESTAMP : TimestampCast,
}