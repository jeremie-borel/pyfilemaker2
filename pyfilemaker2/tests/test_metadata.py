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

import os
basedir = os.path.join(
    os.path.dirname( __file__ ),
    'ressources'
)

class TestFieldType(unittest.TestCase):

    def test_field_cast_types(self):
        # https://{user}:{pswd}@{host}/fmi/xml/fmresultset.xml?-db=TestJeremie&-lay=test_table1&-findall
        b = FmMeta()
        filename = os.path.join(basedir,'./fields_types.xml')
        with open(filename,'rb') as f:
            nodeiter = parse(
                stream = f,
                fm_meta=b,
                only_meta=True, 
            )
            # force node parsing
            tuple(nodeiter)

        map = {
            'f_text': TextCast,
            'f_number':NumberCast,
            'f_time': TimeCast,
            'f_summary' : NumberCast,
            'f_date': DateCast,
            'f_timestamp': TimestampCast,
            'f_container': TextCast,
            'f_calculation_text': TextCast,
            'f_calculation_number': NumberCast,
            'f_multi_text': TextCast,
            'f_multi_number': NumberCast,
        }

        for k,v in map.items():
            f = b.get_fm_field(raw_name=k,table=None)
            self.assertEqual( f.caster.__class__, v )

    def test_data1(self):
        # https://{user}:{pswd}@{host}/fmi/xml/fmresultset.xml?-db=TestJeremie&-lay=test_table1&-findall
        f1 = os.path.join(basedir,'./fields_types.xml')
        nodeiter = parse(
            stream = f1,
        )
        data = [ p for p in nodeiter ]
        out = {
            'f_text': 'pure ascii text',
            'f_number':0.123,
            'f_time': datetime.time( hour=11,minute=53,second=56 ),
            'f_date': datetime.date( year=2018, month=9, day=1 ),
            'f_timestamp': datetime.datetime( year=2018, month=9, day=1, hour=11, minute=54, second=7 ),
        }

        for k,v in out.items():
            self.assertEqual( data[0][k], out[k] )

    def test_data_tz(self):
        f1 = os.path.join(basedir,'./fields_types.xml')
        tz = pytz.timezone('Europe/Zurich')
        d = datetime.datetime( year=2018, month=9, day=1, hour=11, minute=54, second=7 )
        d = tz.normalize( tz.localize( d ) )

        fm = FmMeta(
            server_timezone=tz,
        )
        nodeiter = parse(
            stream = f1,
            fm_meta=fm,
        )
        data = [ p for p in nodeiter ]
        val = data[0]['f_timestamp']

        self.assertEqual( d, val )

    def test_non_ascii_chars(self):
        # https://{user}:{pswd}@{host}/fmi/xml/fmresultset.xml?-db=TestJeremie&-lay=test_awfull_table&-findall
        f1 = os.path.join(basedir,'./non_ascii_chars.xml')
        nodeiter = parse(
            stream = f1,
        )
        data = [ p for p in nodeiter ]
        out = {
            'field1': """éà¢ß
non-secable dash: –
non-secable space: ' '
ç%_
""",
            "ça c'est l'été": 'en été il fait chaud',
        }
        d = data[0]
        for k,v in out.items():
            self.assertEqual( data[0][k], out[k] )


if __name__ == '__main__':
    unittest.main()
