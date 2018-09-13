from __future__ import unicode_literals, absolute_import

class XmlError(Exception):
    pass

class FmError(Exception):
    def __init__( self, msg="", code= -1, version='fms17' ):
        self._msg = msg
        self.code = code
        self.version = version
        super(FmError,self).__init__("taaasdf")

    def __str__( self ):
        msg = self._msg
        if not self._msg:
            from .errors_fms17 import FMErrorNum
            msg = FMErrorNum.get(self.code,'')
        return "FmError<{}>({})".format(self.code,msg or "no message")
