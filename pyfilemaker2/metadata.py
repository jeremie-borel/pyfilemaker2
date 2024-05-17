import logging
from typing import Optional, TYPE_CHECKING, Union
from zoneinfo import ZoneInfo


from .data import MutableDict

from .caster import (
    DummyCast,
    default_cast_map
)

if TYPE_CHECKING:
    from lxml.etree import _Element
    from .caster import TypeCast
    from .server import FmQuery, FmRecord

__all__ = ['FmMeta', 'metadata_parser', 'FmFieldBase', 'FmFieldData']

log = logging.getLogger(__name__)

class FmMeta:
    """
    FmMeta objects hold all the information about an xml query to an FMS server
    - date, time and timestamp formats
    - fields definition and how to cast them into python objects
    - relatedset fields definition
    """

    def __init__(
        self,
        cast_map: dict[str, type['TypeCast']] = {},
        server_timezone: Optional[ZoneInfo] = None
    ):
        self.cast_map = default_cast_map
        if cast_map:
            self.cast_map.update(cast_map)
        self.server_timezone = server_timezone

        # namespace of the xml. http://www.filemaker.com/xml/fmresultset
        self.ns: str = ''

        self._ns_length = 0
        self.database: dict[str, str] = {}
        self.error: Optional[int] = None
        self.date_pattern: str = ''
        self.time_pattern: str = ''
        self.timestamp_pattern: str = ''
        self.db_name: str = ''
        self.layout: str = ''
        self.table: str = ''
        self.encoding = 'utf8'
        self.fms_version: str = ''
        self.total_count: int = -1
        self.fetch_count: int = -1
        self.fields = {}
        self.query: Optional['FmQuery'] = None

    def get_tagname(self, node) -> str:
        """Returns the tag name striped from the ns name at the begining"""
        return node.tag[self._ns_length:]

    def add_field(self, field: 'FmFieldData'):
        """Adds an FmFieldBase object in the current context"""
        raw_name = field.raw_name
        if not raw_name:
            return

        field.splitted_names = raw_name.split('::', 1)
        if field.table and field.table != field.splitted_names[0]:
            field.splitted_names = list([field.table] + field.splitted_names)
        ukey = raw_name
        if field.table:
            ukey = f"{field.table}__{raw_name}"
        if ukey in self.fields:
            log.warning(
                f"One field ({ukey}) exists twice in the layout. This is "
                "a problem because the FmRecord value is set twice and so "
                "will be considered as edited. If this field is a computed "
                "value on FMS and one calls do_edit(record), the call will fail."
            )
        self.fields[ukey] = field

    def get_fm_field(self, raw_name: str, related_table: str) -> 'FmFieldData':
        if not related_table:
            return self.fields[raw_name]
        return self.fields[f"{related_table}__{raw_name}"]

    def set_context(self, name, field=None):
        if name is None or name == '':
            self._context = self.fields
            return
        if name not in self.fields:
            self.fields[name] = field
        self._context = self.fields[name]

    def decode_attrs(self, value: Union[bytes, str]) -> str:
        if not isinstance(value, str):
            return value.decode(self.encoding)
        return value

    def decode_data(self, value: Union[bytes, str, None]) -> Optional[str]:
        if value is not None:
            if not isinstance(value, str):
                return value.decode(self.encoding)
            return value
        return None

    # def sanitize_field_identifier(self, value):
    #     val = unidecode(value)
    #     return val.replace('::','_')

    def parse_fm_dates_formats(self, formats: dict[str, str]) -> dict[str, str]:
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
        def _build_one(stamp) -> str:
            return (
                stamp
                .replace('yyyy', '%Y')
                .replace('MM', '%m')
                .replace('dd', '%d')
                .replace('HH', '%H')
                .replace('mm', '%M')
                .replace('ss', '%S')
            )

        return {
            'date': _build_one(formats.get('date-format', '')),
            'timestamp': _build_one(formats.get('timestamp-format', '')),
            'time': _build_one(formats.get('time-format', '')),
        }

    def get_record_class(self) -> type[MutableDict]:
        return MutableDict

    def __repr__(self) -> str:
        return "<{klass} ({x.db_name},layout:{x.layout}) {error} fields:({fields})>".format(
            klass=self.__class__.__name__,
            x=self,
            error=self.error if self.error else '',
            fields=", ".join(self.fields.keys()),
        )

    def dump(self) -> str:
        """Verbose representation of the object"""
        s = ["  " + field.dump() for field in self.fields.values()]
        return "{klass}(({x.db_name},layout:{x.layout}) {error}):\n{fields}".format(
            klass=self.__class__.__name__,
            x=self,
            error=self.error if self.error else '',
            fields="\n".join(s)
        )


class FmFieldBase:
    def __init__(
        self,
        raw_name: str,
        attrs: dict[str, str] = {},
        fm_meta: Optional[FmMeta] = None,
        table: str = ''
    ):
        self.raw_name = raw_name
        self.attrs = attrs
        self.fm_meta = fm_meta
        self.table = table
        # self.is_multi=1 means not a multi_value field (in the fms sens). >1 means multi_value.
        self.is_multi = 1
        self.splitted_names: list[str] = []

    def set_value(self, record, data_list):
        pass

    def dump(self) -> str:
        """Verbose representation of the object"""
        return "'{}'".format(self.raw_name)


class FmFieldData(FmFieldBase):
    def __init__(
        self,
        raw_name: str,
        attrs: dict[str, str] = {},
        fm_meta: Optional[FmMeta] = None,
        table: str = ''
    ):
        super().__init__(
            raw_name=raw_name,
            attrs=attrs,
            fm_meta=fm_meta,
            table=table
        )
        self.is_multi = 0
        if fm_meta:
            result = attrs.get('result', '')
            caster_class = fm_meta.cast_map.get(result, DummyCast)
            self.caster = caster_class(
                fm_field=self,
                fm_meta=fm_meta,
            )
            self.is_multi = int(attrs.get('max-repeat', '1'))

    def set_value(self, record: dict, data_list: list):
        """
        Set the value into the record.
        """
        value = data_list
        if self.is_multi == 1:
            value = data_list[0]

        if len(self.splitted_names) == 1:
            record[self.splitted_names[0]] = value
        else:
            # if self.splitted_names is ['a',''b','c'] we want to create
            # {'a':{'b':'c': value}}
            
            # If path is ['a','b','c'] and table is 'a' we don't repeat 'a'.
            path = []
            if self.table:
                path.append(self.table)
                if self.splitted_names[0] == self.table:
                    path.extend(self.splitted_names[1:])
            else:
                path = self.splitted_names[:]

            name = path[-1]

            obj = record
            for ctx in path[:-1]:
                try:
                    obj = obj[ctx]
                except KeyError:
                    obj[ctx] = self.fm_meta.get_record_class()()
                    obj = obj[ctx]
            obj[name] = value

    def dump(self) -> str:
        """Verbose representation of the object"""
        return f"{self.raw_name}: type:{self.caster.__class__.__name__}"


class FmFieldContainer(FmFieldBase, dict):
    def __init__(
        self,
        raw_name: str,
        attrs: dict[str, str] = {},
        fm_meta: Optional[FmMeta] = None,
    ):
        super().__init__(raw_name=raw_name,attrs=attrs,fm_meta=fm_meta)

    def dump(self):
        """Verbose representation of the object"""
        s = ["     " + field.dump() for field in self.values()]
        return "Container({})\n{}".format(
            self.raw_name,
            "\n".join(s),
        )


def metadata_parser(node: '_Element', fm_meta: FmMeta, table: str = ''):
    """
    Parses the <metadata> node from the xml.
    :fm_meta: is the FmMeta object that is to be defined
    :node: is either the <metadata> node or a <relatedset-definition> subnode
    :table: is defined only when in a <realtedset-definition> node.
    """
    tag = fm_meta.get_tagname(node)
    if tag == 'metadata':
        for subnode in node:
            metadata_parser(
                node=subnode,
                fm_meta=fm_meta,
                table=table,
            )
    elif tag == 'field-definition':
        f = fm_meta.decode_attrs
        attrs = {f(k): f(v) for k, v in node.attrib.items()}
        raw_name = attrs['name']
        fm_meta.add_field(FmFieldData(
            raw_name=raw_name,
            attrs=attrs,
            fm_meta=fm_meta,
            table=table
        ))

    elif tag == 'relatedset-definition':
        f = fm_meta.decode_attrs
        attrs = {f(k): f(v) for k, v in node.attrib.items()}
        table = attrs['table']

        f = FmFieldContainer(raw_name=table, fm_meta=fm_meta)
        try:
            for subnode in node:
                metadata_parser(
                    node=subnode,
                    fm_meta=fm_meta,
                    table=table,
                )
        except Exception as e:
            raise e
        finally:
            table = ''
