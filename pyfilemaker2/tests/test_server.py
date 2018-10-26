# -*- coding: utf8 -*-
from __future__ import unicode_literals, absolute_import, print_function
import unittest, datetime, requests

# python 2.7/3.x mock package
try:
    import unittest.mock as mock
except ImportError:
    import mock

from pyfilemaker2.server import FmServer
from collections import namedtuple

import os
base = os.path.dirname( __file__ )

TEST_USER = 'xml'
TEST_PASSWORD = 'xml1234'
TEST_URL = 'https://{}:{}@essaim-norma.etat-de-vaud.ch/'.format(TEST_USER, TEST_PASSWORD)
TEST_FILE = 'TestJeremie'

# import urllib3
# urllib3.disable_warnings()

def get_fm_server():
    fm = FmServer(
        url=TEST_URL, 
        db=TEST_FILE,
        request_kwargs={
            'verify': True,
            'stream': True,
            'timeout': 25,
        },
        debug=False,
    )
    return fm

class Dummy():
    def __init__(self, filename):
        self.raw = open( os.path.join(base,filename),'rb' )
    def __del__( self ):
        self.raw.close()
    def raise_for_status(self):
        pass

class TestServerOffline(unittest.TestCase):
    @mock.patch.object( requests, 'get' )
    def test_db_names( self, mock_get ):
        mock_get.return_value = Dummy('./ressources/dbnames.xml')
        fm = FmServer()
        lst = fm.get_db_names()
        self.assertEqual( set(lst), set(['DB1', 'D B2', 'été']))

    @mock.patch.object( requests, 'get' )
    def test_layout_names( self, mock_get ):
        mock_get.return_value = Dummy('./ressources/layout_names.xml')
        fm = FmServer(db='test')
        lst = fm.get_layout_names()
        self.assertEqual( set(lst), set([
            'table2',
            'test_awfull_table',
            'table1',
            'table3',
            'layout1',
            'tableB',
            'tableC',
            'tableA',
            'test_table1',
            'non âscïi Täßle ñame'
        ]))

    @mock.patch.object( requests, 'get' )
    def test_script_names( self, mock_get ):
        mock_get.return_value = Dummy('./ressources/scriptnames.xml')
        fm = FmServer(db='test')
        lst = fm.get_script_names()
        self.assertEqual( set(lst), set([
            'generate_dummy_data',
            'étoile mâtinée',
        ]))

class TestServerOnline(unittest.TestCase):

    def test_find_equal( self ):
        fm = get_fm_server()
        fm.layout = 'test_table_query'
        resultset = fm.do_find( id=1 )
        r = tuple(resultset)

        self.assertEqual( len(r), 1 )

        record = r[0]

        data = {
            'color': 'blue', 
            'stamp': datetime.datetime(2018, 9, 12, 19, 48, 37), 
            'id': 1, 
            'value': 0.7, 
            'day': datetime.date(2018, 9, 12),
            'month': 'September',
        }

        self.assertEqual( record, data )

    def test_find_with_operator( self ):
        fm = get_fm_server()
        fm.layout = 'test_table_query'
        resultset = fm.do_find( value__lt=0.5 )
        r = tuple(resultset)
        self.assertEqual( len(r), 2 )

    def test_find_with_date( self ):
        fm = get_fm_server()
        fm.layout = 'test_table_query'
        date = datetime.datetime( 2018, 9, 12, 21, 0, 0 )
        resultset = fm.do_find( stamp__gt=date )
        r = tuple(resultset)
        self.assertEqual( r[0]['id'], 3 )

    # def test_edit( self ):
    #     fm = get_fm_server()
    #     fm.layout = 'test_table_edit'
    #     resultset = fm.do_find( value__lt=0.5 )
    #     r = tuple(resultset)
    #     self.assertEqual( len(r), 2 )



if __name__ == '__main__':
    unittest.main()
