from collections import Counter

class MutableDict(dict):
    """
    Thin wrapper around dict that tracks keys that are set more than once.

    It also keeps tracks the record_id and the mod_id is applicable
    """
    def __init__( self, *args, **kwargs ):
        super( MutableDict, self ).__init__(*args, **kwargs)
        self.__key_counter = Counter( self.keys() )
        self.record_id = None
        self.mod_id = None

    def __setitem__( self, key, value ):
        self.__key_counter[key] += 1
        super( MutableDict, self ).__setitem__(key, value)

    def __delitem__( self, key ):
        super( MutableDict, self ).__delitem__( key )
        self.__key_counter[key] += 1

    def pop( self, key, default=None ):
        if key in self.keys():
            self.__key_counter[key] += 1
        return super(MutableDict, self).pop(key,default)

    def popitem( self ):
        e = super( MutableDict, self ).popitem()
        self.__key_counter[e[0]] += 1

    def clear(self):
        for k in self.keys():
            self.__key_counter[k] += 1
        return super(MutableDict, self).clear()

    def update(self,other):
        # TODO: implement a more generic way of handling the :other: argument
        # WARNING: atm the x.update(a=22,b=35) syntax is not supported.
        if hasattr(other,'keys'):
            for k in other.keys():
                self.__key_counter[k] += 1
        else:
            try:
                for k,v in other:
                    self.__key_counter[k] += 1
            except Exception as e:
                pass

        return super(MutableDict,self).update(other)


    def setdefault(self,key,default):
        if key in self.keys():
            self.__key_counter[key] += 1
        return super(MutableDict, self).setdefault(key,default)

    def changed_keys(self):
        return tuple( k for k,v in self.__key_counter.items() if v>1 )