#!/usr/bin/env python
# -*- coding: utf8 -*-
from __future__ import unicode_literals, absolute_import, print_function

from lxml import etree

from .errors import FmError, XmlError

from .metadata import (
    metadata_parser,
    FmMeta,
)

def parse( stream, fm_meta=None, only_meta=False ):
    """
    Generator that parses an FMS xml response.

    :fm_meta: FmMeta instance or None.

    :stream: is either a file-like objects (bytestring for python2.7, do not use codecs.open) 
    or a response.raw attribute from a request query.
    """
    if not fm_meta:
        fm_meta = FmMeta()

    tree = etree.iterparse(
            stream,
            # events=("start", "end"),
            events=('start-ns','start', 'end',),
    )

    record = fm_meta.get_record_class()()
    table = None # related table
    field_name = None
    relatedset = []
    data_buffer = []
    decode = fm_meta.decode_data
    caster = None

    for event, elem in tree:
        if event == 'start-ns':
            fm_meta.ns = elem[1]
            fm_meta._ns_length = len(fm_meta.ns)+2
            continue

        elif event == 'end':
            tag = fm_meta.get_tagname(elem)
            if tag == 'data':
                value = decode(elem.text)
                if caster:
                    data_buffer.append( caster.caster(value) )

            elif tag == 'field':
                raw_name = fm_meta.decode_attrs( elem.attrib.get('name', '') ) or None
                try:
                    field = fm_meta.get_fm_field(
                        raw_name=raw_name,
                        table=table
                    )
                    field.set_value( record=record, data_list=data_buffer )
                except KeyError:
                    pass
                data_buffer = []

            elif tag == 'record':
                # end a record. Either in relatedset or not.
                record.record_id = elem.attrib.get('record-id', None)
                record.mod_id = elem.attrib.get('mod-id', None)
                if not table:
                    yield record
                    record = fm_meta.get_record_class()()

                else:
                    relatedset.append(record)

            elif tag == 'relatedset':
                table = None
                record = super_record
                super_record = None

            elif tag == 'error':
                fm_meta.error = elem.attrib.get('code', None)
                if fm_meta.error is None:
                    raise XmlError("Badly formatted error code in the xml")
                fm_meta.error = int(fm_meta.error)
                if fm_meta.error != 0:
                    # code 401 means no record match the request.
                    if fm_meta.error == 401:
                        break
                    raise FmError(code=fm_meta.error)
            
            elif tag == 'datasource':
                # <datasource database="TestJeremie" 
                # date-format="MM/dd/yyyy" 
                # layout="layout1" 
                # table="table1" 
                # time-format="HH:mm:ss" 
                # timestamp-format="MM/dd/yyyy HH:mm:ss" 
                # total-count="4"/>
                fm_meta.database = dict(elem.attrib)
                dict_formats = fm_meta.parse_fm_dates_formats( formats=fm_meta.database )
                fm_meta.date_pattern = dict_formats['date']
                fm_meta.time_pattern = dict_formats['time']
                fm_meta.timestamp_pattern = dict_formats['timestamp']
                fm_meta.db_name = fm_meta.database['database']
                fm_meta.layout = fm_meta.database['layout']
                fm_meta.table = fm_meta.database['table']

            elif tag == 'metadata':
                metadata_parser( node=elem, fm_meta=fm_meta )
                if only_meta:
                    break

        elif event == 'start':
            tag = fm_meta.get_tagname( elem )

            if tag == 'field':
                try:
                    caster = fm_meta.get_fm_field( 
                        raw_name=fm_meta.decode_attrs(elem.attrib['name']),
                        table=table
                    )
                except KeyError:
                    pass

            elif tag == 'relatedset':
                table = elem.attrib['table']
                record[table] = []
                relatedset = record[table]
                super_record = record
                record = fm_meta.get_record_class()()
            
            elif tag == 'resultset':
                try:
                    _count = int( elem.attrib['count'] )
                    fetch_size = int( elem.attrib['fetch-size'] )
                except (ValueError, TypeError, KeyError):
                    _count = 0
                    fetch_size = 0
                fm_meta.total_count = _count
                fm_meta.fetch_count = fetch_size