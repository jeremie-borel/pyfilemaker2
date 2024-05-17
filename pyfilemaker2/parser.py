from typing import Generator, Optional, TYPE_CHECKING
from lxml import etree


from .errors import FmError, XmlError

from .metadata import (
    metadata_parser,
    FmMeta,
)

if TYPE_CHECKING:
    from pyfilemaker2.data import MutableDict


__all__ = ['parse']


def parse(
    stream,
    fm_meta: Optional[FmMeta] = None,
    only_meta: bool = False,
) -> Generator[dict, None, None]:
    """
    Generator that parses an FMS xml response.

    :fm_meta: FmMeta instance or None.

    :stream: is either a file-like objects or a response.raw attribute from a
    request query.
    """
    if not fm_meta:
        fm_meta = FmMeta()

    tree = etree.iterparse(
        stream,
        events=('start-ns', 'start', 'end'),
    )

    record = fm_meta.get_record_class()()
    reltable = ''  # related table
    relatedset = []
    data_buffer = []
    caster = None
    super_record = None

    for event, elem in tree:
        if event == 'start-ns':
            fm_meta.ns = elem[1]
            fm_meta._ns_length = len(fm_meta.ns)+2
            continue

        elif event == 'end':
            tag = fm_meta.get_tagname(elem)
            if tag == 'data':
                value = fm_meta.decode_data(elem.text)
                if caster:
                    data_buffer.append(caster.caster(value))

            elif tag == 'field':
                raw_name = fm_meta.decode_attrs(
                    elem.attrib.get('name', '')
                )
                try:
                    field = fm_meta.get_fm_field(
                        raw_name=raw_name,
                        related_table=reltable
                    )
                    field.set_value(record=record, data_list=data_buffer)
                except KeyError:
                    pass
                data_buffer = []

            elif tag == 'record':
                # end a record. Either in relatedset or not.
                record.record_id = elem.attrib.get('record-id', None)
                record.mod_id = elem.attrib.get('mod-id', None)
                if not reltable:
                    yield record
                    record = fm_meta.get_record_class()()

                else:
                    relatedset.append(record)
                    record = fm_meta.get_record_class()()

            elif tag == 'relatedset':
                reltable = ''
                record: 'MutableDict' = super_record
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
                dict_formats = fm_meta.parse_fm_dates_formats(
                    formats=fm_meta.database
                )
                fm_meta.date_pattern = dict_formats['date']
                fm_meta.time_pattern = dict_formats['time']
                fm_meta.timestamp_pattern = dict_formats['timestamp']
                fm_meta.db_name = fm_meta.database['database']
                fm_meta.layout = fm_meta.database['layout']
                fm_meta.table = fm_meta.database['table']

            elif tag == 'metadata':
                metadata_parser(node=elem, fm_meta=fm_meta)
                if only_meta:
                    break

            elif tag == 'product':
                fm_meta.fms_version = elem.attrib.get('version', '')

        elif event == 'start':
            tag = fm_meta.get_tagname(elem)

            if tag == 'field':
                try:
                    caster = fm_meta.get_fm_field(
                        raw_name=fm_meta.decode_attrs(elem.attrib['name']),
                        related_table=reltable
                    )
                except KeyError:
                    pass

            elif tag == 'relatedset':
                reltable:str= elem.attrib['table']
                record[reltable] = []
                relatedset = record[reltable]
                super_record = record
                record = fm_meta.get_record_class()()

            elif tag == 'resultset':
                try:
                    _count = int(elem.attrib['count'])
                    fetch_size = int(elem.attrib['fetch-size'])
                except (ValueError, TypeError, KeyError):
                    _count = 0
                    fetch_size = 0
                fm_meta.total_count = _count
                fm_meta.fetch_count = fetch_size
