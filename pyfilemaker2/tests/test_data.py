# -*- coding: utf8 -*-
from __future__ import unicode_literals, absolute_import, print_function
import unittest

from pyfilemaker2.data import MutableDict

class TestMutableDict(unittest.TestCase):

    def test_empty_is_unchanged(self):
        b = MutableDict()
        self.assertEqual( b.changed_keys(), () )

    def test_behave_as_dict(self):
        b = MutableDict()
        b['43'] = 'something'
        self.assertEqual( b['43'], 'something' )

    def test_track_change(self):
        b = MutableDict()
        b['43'] = 'something'
        b['42'] = 12
        b['other'] = 234

        self.assertEqual( b.changed_keys(), () )

        b['43'] = 'whatever'
        b['42'] = 12
        b['22'] = 12

        self.assertEqual( set(b.changed_keys()), set(['43', '42']) )

    def test_track_change_at_init(self):
        b = MutableDict([(1,2),(3,4),(4,5)])
        b[3] = 2
        self.assertEqual( b.changed_keys(), (3,) )

    def test_track_deletion(self):
        b = MutableDict()
        b['43'] = 'something'

        del b['43']

        self.assertEqual( b.changed_keys(), ('43',) )

    def test_track_deletion2(self):
        # non existing keys should not trigger a change
        b = MutableDict()
        b['43'] = 'something'

        try:
            del b['25']
        except KeyError:
            pass
        try:
            del b['25']
        except KeyError:
            pass

        self.assertEqual( b.changed_keys(), () )

    def test_track_pop(self):
        # non existing keys should not trigger a change
        b = MutableDict()

        b[4] = 23
        b.pop(4)
        b.pop(22,None)
        b.pop(22,None)

        self.assertEqual( b.changed_keys(), (4,) )

    def test_update1( self ):
        b = MutableDict( {'a':1,'b':2} )
        other = { 'a':3,'c':4 }
        b.update(other)

        self.assertEqual( b.changed_keys(), ('a',) )

    def test_update1( self ):
        b = MutableDict( {'a':1,'b':2} )
        b.update( [('a',3),('c',4)] )

        self.assertEqual( b.changed_keys(), ('a',) )


if __name__ == '__main__':
    unittest.main()
