# -*- coding: utf8 -*-
import unittest
import datetime
import requests

import mock
from pyfilemaker2.server import FmServer

import os
base = os.path.dirname(__file__)

try:
    # define in this module the constants required to access the FM database
    from pyfilemaker2.tests.ressources.fms_test_server import (
        TEST_USER,
        TEST_PASSWORD,
        TEST_SERVER,
        TEST_PROTOCOL,
        TEST_FILE,
    )
except ModuleNotFoundError:
    # if the module is not defined, use the following default settings
    TEST_USER = 'xml'
    TEST_PASSWORD = 'xml1234'
    TEST_SERVER = 'test.hostname.fms'
    TEST_PROTOCOL = 'https'
    TEST_FILE = 'TestFile'


def get_fm_server(server=TEST_SERVER):
    fm = FmServer(
        url='{}://{}:{}@{}/'.format(TEST_PROTOCOL, TEST_USER, TEST_PASSWORD, TEST_SERVER),
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
        self.raw = open(os.path.join(base, filename), 'rb')

    def __del__(self):
        self.raw.close()

    def raise_for_status(self):
        pass


class TestServerOffline(unittest.TestCase):
    @mock.patch.object(requests, 'get')
    def test_db_names(self, mock_get):
        mock_get.return_value = Dummy('./ressources/dbnames.xml')
        fm = FmServer()
        lst = fm.get_db_names()
        self.assertEqual(set(lst), set(['DB1', 'D B2', 'été']))

    @mock.patch.object(requests, 'get')
    def test_layout_names(self, mock_get):
        mock_get.return_value = Dummy('./ressources/layout_names.xml')
        fm = FmServer(db='test')
        lst = fm.get_layout_names()
        self.assertEqual(set(lst), set([
            'table2',
            'test_awfull_table',
            'table1',
            'table3',
            'layout1',
            'tableB',
            'tableC',
            'tableA',
            'test_table1',
            'non âscïi Täßle ñame',
            'test_rub_calc',
        ]))

    @mock.patch.object(requests, 'get')
    def test_script_names(self, mock_get):
        mock_get.return_value = Dummy('./ressources/scriptnames.xml')
        fm = FmServer(db='test')
        lst = fm.get_script_names()
        self.assertEqual(set(lst), set([
            'generate_dummy_data',
            'étoile mâtinée',
        ]))


class TestServerOnline(unittest.TestCase):
    def test_find_equal(self):
        fm = get_fm_server()
        fm.layout = 'test_table_query'
        resultset = fm.do_find(id=1)
        r = tuple(resultset)

        self.assertEqual(len(r), 1)

        record = r[0]

        data = {
            'color': 'blue',
            'stamp': datetime.datetime(2018, 9, 12, 19, 48, 37),
            'id': 1,
            'value': 0.7,
            'day': datetime.date(2018, 9, 12),
            'month': 'September',
        }

        self.assertEqual(record, data)

    def test_find_with_operator(self):
        fm = get_fm_server()
        fm.layout = 'test_table_query'
        resultset = fm.do_find(value__lt=0.5)
        r = tuple(resultset)
        self.assertEqual(len(r), 2)

    def test_find_with_date(self):
        fm = get_fm_server()
        fm.layout = 'test_table_query'
        date = datetime.datetime(2018, 9, 12, 21, 0, 0)
        resultset = fm.do_find(stamp__gt=date)
        r = tuple(resultset)
        self.assertEqual(r[0]['id'], 3)

    def test_write_with_readonly_data(self):
        fm = get_fm_server()
        fm.layout = 'test_rub_calc'
        resultset = fm.do_find(a_texte='un', b_nombre=1)
        r = tuple(resultset)
        item = r[0]
        test_read = item['c_concat']
        self.assertEqual(test_read, 'un1')

        item['a_texte'] = 'voila'
        fm.do_edit(item)
        item['a_texte'] = 'un'
        fm.do_edit(item)

    # def test_edit(self):
    #     fm = get_fm_server()
    #     fm.layout = 'test_table_edit'
    #     resultset = fm.do_find(value__lt=0.5)
    #     r = tuple(resultset)
    #     self.assertEqual(len(r), 2)


if __name__ == '__main__':
    unittest.main()
