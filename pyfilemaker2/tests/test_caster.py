# -*- coding: utf8 -*-
from __future__ import unicode_literals, absolute_import, print_function
import unittest, datetime, pytz

from pyfilemaker2.metadata import FmMeta
from pyfilemaker2.parser import parse
from pyfilemaker2.caster import (
    TextCast,
    NumberCast,
    DateCast,
    TimeCast,
    TimestampCast,
    BackCast,
)

class TestBackCast(unittest.TestCase):

    def test_backcast_timezone( self ):
        tz = pytz.timezone('Europe/Zurich')
        d = datetime.datetime( year=2018, month=9, day=1, hour=11, minute=54, second=7 )
        d = tz.normalize( tz.localize( d ) )

        tz2 = pytz.UTC
        bc = BackCast()
        bc.tz = tz2
        
        self.assertEqual( bc( field=None, value=d ), '09/01/2018 09:54:07' )

    def test_backcast_timezone2( self ):
        tz2 = pytz.UTC
        d = datetime.datetime( year=2018, month=9, day=1, hour=11, minute=54, second=7 )
        d = tz2.normalize( tz2.localize( d ) )

        tz = pytz.timezone('Europe/Zurich')
        bc = BackCast()
        bc.tz = tz
        self.assertEqual( bc( field=None, value=d ), '09/01/2018 13:54:07' )

    def test_backcast_timezone3( self ):
        d = datetime.datetime( year=2018, month=9, day=1, hour=11, minute=54, second=7 )

        tz = pytz.timezone('Europe/Zurich')
        bc = BackCast()
        bc.tz = tz
        # naive must stay naive :/
        self.assertEqual( bc( field=None, value=d ), '09/01/2018 11:54:07' )


if __name__ == '__main__':
    unittest.main()
