#!/usr/bin/env python
# -*- coding: utf8 -*-
from __future__ import unicode_literals, absolute_import, print_function

from future.utils import python_2_unicode_compatible, raise_with_traceback
# overloaded by 'future' in python 2.7
from builtins import str, int

# following two lines handle compatibility for urllib module
# may be erased once python2.7 compat is dropped.
from future.standard_library import install_aliases
install_aliases()
from urllib.parse import urlparse, urlencode

import requests, logging, copy, re

from .metadata import FmMeta
from .errors import FmError
from .parser import parse
from .data import MutableDict
from .caster import BackCast

log = logging.getLogger( __name__ )

__all__ = ['FmServer',]

class FmServer(object):
    """
    Main class to interact with FM server instances.

    Note that all the static arguments can also be 
    passed in the **kwargs from the FmServder.__init__'s method.
    """
    meta_class = FmMeta
    cast_map = None
    back_cast_class = BackCast
    request_kwargs = {
        'stream' : True,
        'verify': True,
        'timeout': 25,
    }
    server_timezone = None

    def __init__(
        self, 
        url='http://login:password@localhost/fmi/xml/fmresultset.xml', 
        db='', 
        layout='', 
        debug=False,
        **kwargs
    ):
        o = urlparse( url )
        path = o.path
        if path == '/':
            path = None
        self.url = {
            'raw': o,
            'hostname': o.hostname,
            'username': o.username,
            'password': o.password,
            'scheme': o.scheme or 'http',
            'port': o.port or (443 if o.scheme=='https' else 80),
            'path': path or '/fmi/xml/fmresultset.xml',
        }
        
        self.db = db
        self.layout = layout

        self.debug = debug
        self.fm_meta = None

        self.options = {}
        optkeys = (
                'meta_class', 
                'back_cast_class',
                'cast_map',
                'request_kwargs',
                'server_timezone'
            )
        for key in optkeys:
            if key in kwargs:
                self.options[key] = copy.copy( kwargs[key] )
            else:
                self.options[key] = copy.copy( getattr( self.__class__, key ) )

    def get_db_names(self):
        """Returns the list of databases available through xml"""

        query = FmQuery(action='-dbnames', fm_server=self)
        stream = self._do_request(query)
        return tuple( v['DATABASE_NAME'] for v in stream )

    def get_layout_names(self):
        """Returns a tuple of all the available layouts in a db."""
        query = FmQuery(action='-layoutnames', fm_server=self)
        stream = self._do_request(query)
        return tuple( v['LAYOUT_NAME'] for v in stream )

    def get_script_names(self):
        """Retrurns a tuple of all the scripts"""
        query = FmQuery(action='-scriptnames', fm_server=self)
        stream = self._do_request(query)
        return tuple( v['SCRIPT_NAME'] for v in stream )
        
    def get_field_names(self):
        """Returns a tuple of all the fields accessible in the layout"""
        self.do_view()
        return tuple( self.fm_meta.fields.keys() )

    def get_file( self, file_xml_uri ):
        """Fetches container data from an FM server
        
        e.g. on file mydb.fmp12, a layout 'my_layout' has a field 'join'
        of type container

            fm = FmServer( db='mydb', layout=my_layout )
            record = fm.do_find_any()
            name, extension, data = fm.get_file( record['join'] )
            out = open( "{}.{}".format(name, extension), 'w' )
            out.write( data )
            out.close()
        """
        # added % and , chars in the list of allowed chars so that html 
        # special char are possible in the file name. (19.1.2016, blj)
        find = re.match('/fmi/xml/cnt/(?P<name>[,%\w\d.-]+)\.(?P<ext>[\w]+)[?]-', file_xml_uri) 
        file_name = find.group('name')
        file_extension = find.group('ext')
        file_binary = self._do_request(is_file=True, query=file_xml_uri)
        return (file_name, file_extension, file_binary)

    def do_script(self, script_name, param=None, return_all=True):
        """
        Triggers the excution of script :script_name: on the FM server.
        Note that a script always returns the results from the layout with findall.

        Returns an iterator.

        If you do not need the found results, you may set :return_all: to False
        in which case only the first result will be returned by the xml.

        Moreover, note that when executing

          result = fm.do_script( 'myname' )
        
        the script is executed but the returned result are not yet parsed. This occurs only
        when one evaluates the generator, e.g.

          for item in result:
              ...
        
        if :param: is not None, it is passed as argument to the FM server. If you need multiple
        arguments, concatenate them in python into a single one and split them back in the FM
        script.
        """
        query = FmQuery(action='-findall', fm_server=self)
        if not return_all:
            query.pre_find( max=1 )

        query.add_args(name='-script', value=script_name )
        if param:
            query.add_param( name='-script.param', value=param )
        
        stream = self._do_request(query)
        return stream

    def do_script_after(self, func, func_kwargs={}, script_name='', params=None):
        raise NotImplementedError("not yet implemented")

    def fetched_records_number( self ):
        """Returns the number of result in the resultset
        
        ATM available only once the first result has been parsed, not before.
        """
        return self.fm_meta.fetch_count

    def total_records_number( self ):
        """Returns the number of result that would be returned in a 
        do_find_all request.

        ATM available only once the first result has been parsed, not before. E.g.

          fm = FmServer( ... )
          fm.do_find_any()     # queries and parses the resultset
          fm.total_records_number()
        """
        return self.fm_meta.total_count

    def do_find_query(self, query_dict, skip=None, max=None, sort=[], paginate=None ):
        """
        Allows to do more complex queries than do_find.
        Operators in the Django like syntax like size__gt=22 are not possible in this 
        mode but one can still use FM operators using e.g. 'size'='>22'

        # search for blue color houses:
        fm.do_find_query( {'color':'blue'} )

        # search for blue colored OR big houses:
        fm.do_find_query( {'color':'blue', 'size':'big'} )

        # search for houses with ids either 1 or 2 
        # note that the syntax "where id IN [1,2,...]" does not exist in FMS so
        # these kind of query are effectively evaluated as successive OR.
        fm.do_find_query( {'id':[1,2]} )

        # search for id = 1 or 2 or color=blue
        fm.do_find_query( {'id':[1,2], 'color':'blue'} )

        # each argument must be though as an FMS research and thus can be negated (omit records)
        # with a '!' in front of it.
        # search for id = 1 or 2 or color!=blue
        fm.do_find_query( {'id':[1,2], '!color':'blue'} )

        # Doing AND queries requires to nest the parameters:
        # search for 'blue' AND 'big' houses
        fm.do_find_query({
            '1': {'color':'blue','size':'big'},
        })
        # note that the key '1' is arbitrary, it is not even sent to the xml.
        # Furthermore, in this form, search values (e.g. 'blue') must be single values. They
        # cannot be arrays like in the previous example.

        # search for (color='blue' AND size='big') OR stairs=0 houses 
        fm.do_find_query({
            '1': {'color':'blue','size':'big'},
            '2': {'stairs':0},
        })

        # coumpound queries can be negated but then their order matters, hence
        # one should use a collections.OrderedDict instead of a dict.
        # To fully understand the logic here, test on a FM table manually...
        # search for ('blue' AND 'big') excluding houses with stairs=1.
        fm.do_find_query( OrderedDict([
            ('1', {'color':'blue','size':'big'}),
            ('!2': {'stairs':1}),
        ]))

        # search for houses not with stairs=1 (e.g. stairs!=1) OR (color='blue' AND size='big')
        fm.do_find_query( OrderedDict([
            ('!2', {'stairs':1}),
            ('1', {'color':'blue','size':'big'}),
        ]))
        """
        count = 1
        query_list = []
        query_values = []

        for key, value in query_dict.items():
            # test whether the request is negated.
            neg = ''
            if key.startswith( '!'):
                neg = '!'
                key = key[1:]

            # a dict means neasted AND queries
            if isinstance( value, (dict,) ):
                # all the queries that will be ANDed
                sub_query_list = []
                for sub_key, sub_value in value.items():
                    sub_query_list.append( 'q{}'.format(count) )
                    query_values.append( ['-q{}'.format(count), sub_key] )
                    query_values.append( ['-q{}.value'.format(count), sub_value] )
                    count += 1
                query_list.append( '{}({})'.format(neg, ",".join(sub_query_list))  )

            # the value is a tuple, do an OR or all the arguments
            elif isinstance( value, (tuple,list,set) ):
                for sub_value in value:
                    query_list.append( '{}(q{})'.format(neg, count)  )
                    query_values.append( ['-q{}'.format(count), key] )
                    query_values.append( ['-q{}.value'.format(count), sub_value] )
                    count += 1
            else:
                query_list.append( '{}(q{})'.format(neg, count)  )
                query_values.append( ['-q{}'.format(count), key] )
                query_values.append( ['-q{}.value'.format(count), value] )
                count += 1

        query = FmQuery(action='-findquery', fm_server=self)
        query.pre_find( sort=sort, skip=skip, max=max )
        query.add_args( '-query', ";".join( i for i in query_list ) )
        for k,v in query_values:
            query.add_param( k,v, parse_operator=False )

        stream = self._do_request(query, paginate=paginate )
        return stream


    def do_find(self, what={}, sort=[], skip=None, max=None, lop='AND', paginate=None, **kwargs):
        """Performs a find query on the FM server

        search criterions can be specified through the :what: parameters or 
        through the kwargs arguments. The two forms can be combined. The :kwargs:
        forms as precedence.

        # e.g. 
        fm = FmServer( db='thedb', layout='layout_name')

        # search for records with field 'id' equals 4
        fm.do_find({'id':4})
        # or
        fm.do_find( id=4 )

        # all arguments search criterions are combined either through 
        # AND (default) or through OR depending on the :lop: value.
        # E.g. search for id=4 or color='blue'
        fm.do_find( id=4, color='blue', lop='or' )

        # operators can be specified in the fields name, e.g.
        # search for id>4
        fm.do_find( id__gt=4 )
        # in this syntax the operators are one of 
        #   __gt, 
        #   __gte, 
        #   __lt, 
        #   __lte,
        #   __endswith,
        #   __beginswith,
        #   __equals,
        #   __contains,
        #   __does_not_contains,

        # Operators can also be specified in the field value. In the latter case,
        # the FMP syntax must be used (see the FM doc for those) e.g.
        fm.do_find( id='4...77' )   # for ids in the range 4 to 77. Or
        fm.do_find( id='>4' )       # for ids bigger than 4.
        fm.do_find( id='==' )       # for records with no id.

        # Note that by default (no operator specified), The FM server assumes a 
        # __beginswith operator for string data and a __equals for numeric data. 
        # Which means that 
        fm.do_find( id=4 )
        # will match id=4 but also id=47 if the 'id' field is defined as a string 
        # but not if it is defined as a number. Worse, if your 'id' field is numeric 
        # but it contains something like '4ac', it will match the query and the default
        # type caster of this library will convert it to 'nan' (the object 'not a number')

        :sort: argument can contain a list of sort fields. E.g.
        # to sort first by id and a second sort criterion by date in decreasing order:
        fm.do_find( id__gt=4, sort=['id', '-date'] )
        # another equivalent syntax is:
        fm.do_find( id__gt=4, sort=[('id','ascend'), ('date','descend')] )

        """
        query = FmQuery(action='-find', fm_server=self)
        query.pre_find( sort=sort, skip=skip, max=max, lop=lop )
        for key, value in what.items():
            query.add_param( key, value )
        for key, value in kwargs.items():
            query.add_param( key, value )
        stream = self._do_request(query, paginate=paginate )
        return stream

    def do_find_all(self, sort=[], skip=None, max=None, paginate=None ):
        """Finds all records in the layout."""
        query = FmQuery(action='-findall', fm_server=self)
        query.pre_find( sort=sort, skip=skip, max=max )
        stream = self._do_request(query, paginate=paginate )
        return stream

    def do_find_any(self, what={}, sort=[], lop='AND', **params):
        """Finds all records in the layout."""
        query = FmQuery(action='-findany', fm_server=self)
        query.pre_find( sort=sort, lop=lop )
        stream = tuple( self._do_request(query) )
        if stream and len(stream) > 0:
            return stream[0]
        return []

    def do_delete(self, what, error_if_missing=True ):
        """
        Deletes the record identified by the rec-id attribute of the xml
        E.g.
        # if one knows the rec-id attribute:
        fm = FmServer( ... )
        fm.do_delete( what=34 )        

        # or explicitely deletes a record:
        fm = FmServer( ... )
        results = fm.do_find( id=22 )
        record = tuple(results)[0]
        fm.do_delete( what=record )

        if :error_is_missing: is True it raises an error FmError(code=101)
        when the specified record did not exist.
        """
        recid = None
        if hasattr(what, 'record_id'):
            recid = what.record_id
        else:            
            recid = what
        query = FmQuery(action='-delete', fm_server=self)
        query.add_args( name='-recid', value=recid )
        # force parsing
        try:
            tuple( self._do_request(query) )
        except FmError as e:
            if e.code == 101:
                if not error_if_missing:
                    return None
            raise_with_traceback(e)

    def do_edit(self, what=None, **kwargs):
        """
        Edits a record.
        Trick to editate multi value fields:

        fm.do_edit( what={'record_id':rec.record_id, 'dataC(3)':'bijour'} )

        """
        query = FmQuery(action='-edit', fm_server=self)
        self._parse_what_and_kwargs(what=what,kwargs=kwargs,query=query)
        tuple( self._do_request(query) )


    def do_new(self, what=None, **kwargs):
        """Create a new record, :new: is a dict with fields value pairs.
        One can also use :kwargs: to specify fields' value."""
        query = FmQuery(action='-new', fm_server=self)
        self._parse_what_and_kwargs(what=what,kwargs=kwargs,query=query)
        tuple( self._do_request(query) )


    def do_view(self):
        """Executes the -view action. I.e. it returns the metadata of a given 
        layout with an empty recordset.
        """
        query = FmQuery(action='-view', fm_server=self)
        tuple( self._do_request(query) )

    def do_dup(self, what={} ):
        """Duplicates the given record. :what: can be a 
        data.MutableDict or a record-id.
        """
        query = FmQuery(action='-dup', fm_server=self)
        if isinstance( what, MutableDict ):
            query.add_args( name='-recid', value=what.record_id )
            query.add_args( name='-modid', value=what.mod_id )
        else:
            query.add_args( name='-recid', value=what )
        tuple( self._do_request(query) )

    def _parse_what_and_kwargs( self, query, what, kwargs ):
        if isinstance( what, MutableDict ):
            query.add_args( name='-recid', value=what.record_id )
            query.add_args( name='-modid', value=what.mod_id )

            for key in what.changed_keys():
                if isinstance( what[key], MutableDict ):
                    m = "Can't handle yet the editing of related "
                    "paramters '{}' (e.g table2::fieldX). Skipping it.".format(key)
                    raise ValueError(m)
                if isinstance( what[key], (list,tuple,set) ):
                    m = "Can't set multivalue field '{}' this way. See "
                    "the doc of do_edit for a workaround".format(key)
                    raise ValueError(m)

                query.add_param( name=key, value=what[key], parse_operator=False )
        elif isinstance( what, dict ):
            # if what is a dict, push all arguments into kwargs as
            # kwargs values must take precedence (could be changed one day ?)
            args = copy.copy(what)
            args.update( kwargs )
            kwargs = args

        if 'record_id' in kwargs:
            query.add_args( name='-recid', value=kwargs.pop('record_id') )
            if 'mod_id' in kwargs:
                query.add_args( name='-modid', value=kwargs.pop('mod_id') )
        
        for key,value in kwargs.items():
            query.add_param( name=key, value=value )

    def _build_url(self):
        tmpl = "{scheme}://{hostname}:{port}{path}"
        return tmpl.format( **self.url )

    def _build_file_url(self, xml_req):
        """Builds url for fetching the files from FM."""
        return '{scheme}://{hostname}:{port}{xml_req}'.format(
            xml_req=xml_req,
            **self.url
        )

    def _do_request(self, query, is_file=False, paginate=None ):
        if is_file:
            url = self._build_file_url( xml_req=query )
        else:
            url = self._build_url()
            url = url + '?' + query.format()


        if paginate and not is_file:
            if '-skip' in query.args or '-max' in query.args:
                raise AttributeError("One cannot specify a skip "
                "or a max value in pagination mode.")
            return _paginate( fm_server=self, query=query, page_size=paginate )

        if self.debug:
            # log.info("FmServer({})".format(url))
            print("FmServer({})".format(url))
            if not is_file:
                for item in query.request:
                    print( "  ", item )

        resp = requests.get(
            url = url,
            auth = (self.url.get('username',''), self.url.get('password','')),
            **self.options['request_kwargs']
        )
        # does this breaks streamed response ?
        resp.raise_for_status()

        if is_file:
            return resp.content

        # storing the fm_meta objects for later use
        self.fm_meta = self.options['meta_class']( 
            cast_map=self.options['cast_map'],
            server_timezone = self.options['server_timezone'],
        )
        self.fm_meta.query = query
        # parse returns a generator, so no try catch can be done here.
        return parse(
            stream = resp.raw,
            fm_meta = self.fm_meta,
        )


class FmQuery(object):
    """This class is internal to FmServer. It is used to
    define the arguements that can or must be passed with a 
    given action and it formats and casts theses arguments before
    building a request url.
    """
    _scripts = ['-script','–script.param','-script.prefind','-script.prefind.param', '-script.presort','–script.presort.param']
    _layr = ['-lay.response']
    _finds = ['-recid','-lop','-op','-max','-skip','-sortorder','-sortfield']

    actions = {
        '-dbnames': {
            'required': [],
            'optional': [],
            'params': False,
        },
        '-delete': {
            'required': ['-db','-lay','-recid'],
            'optional': _layr + _scripts,
            'params': False,
        },
        '-dup': {
            'required': ['-db','-lay','-recid'],
            'optional': ['–relatedsets.max'] + _scripts,
            'params': False,
        },
        '-edit': {
            'required': ['-db','-lay','-recid'],
            'optional': ['-modid','-lay.response','–delete.related','–relatedsets.max'] + _scripts,
            'params': True,
        },
        '-find': {
            'required': ['-db','-lay'],
            'optional': ['–relatedsets.max'] + _layr + _finds + _scripts,
            'params': True,
        },
        '-findall': {
            'required': ['-db','-lay'],
            'optional': _layr + _finds + _scripts,
            'params': True,
        },
        '-findany': {
            'required': ['-db','-lay'],
            'optional': _layr + _finds + _scripts,
            'params': True,
        },
        '-findquery': {
            'required': ['-db','-lay','-query'],
            'optional': ['-max','-skip','-sortorder','-sortfield'] + _layr + _scripts,
            'params': True,
        },
        '-layoutnames': {
            'required': ['-db'],
            'optional': [],
            'params': False,
        },
        '-new': {
            'required': ['-db','-lay'],
            'optional': ['–relatedsets.max'] + _layr + _scripts,
            'params': True,
        },
        '-scriptnames': {
            'required': ['-db'],
            'optional': [],
            'params': False,
        },
        '-view': {
            'required': ['-db','-lay'],
            'optional': _scripts,
            'params': False,
        },
    }

    _operators = {
        'endswith': 'ew',
        'beginswith': 'bw',
        'equals': 'eq',
        'contains': 'cn',
        'does_not_contains': 'neq',
        'gt': 'gt',
        'gte': 'gte',
        'lt': 'lt',
        'lte': 'lte',
    }

    def __init__(self,action=None,fm_server=None):
        if action not in self.__class__.actions:
            raise FmError("Invalid action name ({})".format(action))
        self.fm_server = fm_server
        self.action = action
        self.grammar = self.__class__.actions[self.action]
        self.args = {
            '-db': self.fm_server.db,
            '-lay': self.fm_server.layout,
        }
        self._params = []
        self.back_cast = None

    def set_max( self, value ):
        try:
            _max = int(value)
        except ValueError:
            raise FmError("Max value must be a number (got {})".format(value))
        if _max < 0:
            raise FmError("Max value must be positive (got {})".format(_max))
        self.args['-max'] = _max

    def set_skip( self, value ):
        try:
            _skip = int(value)
        except ValueError:
            raise FmError("Skip value must be a number (got {})".format(value))
        if _skip < 0:
            raise FmError("Skip value must be positive (got {})".format(_skip))
        self.args['-skip'] = _skip

    def add_sort_params( self, sort ):
        for i,item in enumerate(sort):
            if isinstance( item, str ):
                field = item
                sort_order = 'ascend'
                if field[0] == '-':
                    field = field[1:]
                    sort_order = 'descend'
            else:
                field, sort_order = item

            if sort_order == '<':
                sort_order = 'ascend'
            elif sort_order == '>':
                sort_order = 'descend'
            self._params.append( ('-sortfield.{}'.format(i+1),field) )
            if sort_order:
                self._params.append( ('-sortorder.{}'.format(i+1),sort_order) )

    def pre_find(self, sort=[], skip=None, max=None, lop='AND'):
        """This function will process attributtes for all -find* commands."""
        self.add_sort_params(sort)
        if skip: self.set_skip(skip)
        if max: self.set_max(max)
        if lop:
            if lop.lower() not in ['and', 'or']:
                raise FMError('Unsupported logical operator (not one of "and" or "or").')
            self.args['-lop'] = lop.lower()

    def add_args( self, name, value ):
        self.args[name] = value

    def add_param(self, name, value, parse_operator=True ):
        """Adds a database parameter"""

        # finding the table and operator parts if any
        parts = name.split('__')
        op = None
        if parse_operator and len(parts)>1 and parts[-1] in self.__class__._operators:
            op = self.__class__._operators[ parts[-1] ]
            parts = parts[:-1]

        field = '::'.join(parts)

        # the value is casted back before being re-injected into xml.
        if not self.back_cast:
            bc = self.fm_server.options['back_cast_class']
            self.back_cast = bc( fm_server=self.fm_server )
        casted_value = self.back_cast( field=field,value=value )

        self._params.append( (field, casted_value) )
        if op:
            self._params.append( ("{}.op".format(field), op) )

    def is_allowed( self, name ):
        self.grammar = self.__class__.actions[self.action]
        if name in self.grammar.required or name in self.grammar.optional:
            return True
        return False

    def format(self):
        if not self.action:
            raise FmError("Empty action name")
        request = []
        args = copy.copy(self.args)
        params = copy.copy(self._params)
        for key in self.grammar['required']:
            if key not in args:
                raise ValueError("A required argument ({}) is not present"
                  " for action {}".format(key, self.action))

            value = args.pop(key)
            if not value:
                raise ValueError("A required argument ({}) is empty ({})".format(key,value))
            request.append( ( key, value ) )

        if self.grammar['params']:
            for key,value in params:
                request.append( (key, value) )

        for key in self.grammar['optional']:
            if key in args:
                request.append( (key, args.pop(key) ) )

        request.append( (self.action,'') )
        self.request = request

        # if args:
        #     log.warning( "Request did not consumed all its arguemnts ({})".format(args) )

        return urlencode( request, doseq=False )

def _paginate( fm_server, query, page_size, current=0 ):
    fm_server_copy = copy.copy(fm_server)
    query = copy.copy(query)

    query.set_skip( current )
    query.set_max( page_size )
    for item in fm_server_copy._do_request( query, is_file=False, paginate=None ):
        fm_server.fm_meta = fm_server_copy.fm_meta
        yield item
    N = fm_server_copy.fetched_records_number()
    if N < page_size:
        return
    
    del fm_server_copy

    for item in _paginate( fm_server, query, page_size, current=current+page_size ):
        yield item

