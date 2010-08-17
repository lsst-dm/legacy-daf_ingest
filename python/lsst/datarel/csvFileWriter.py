import gzip
import re
import lsst.daf.base as dafBase

class CsvFileWriter(object):
    def __init__(self, path, overwrite=True, compress=True):
        if compress:
            self.f = gzip.open(path + ".gz", "w" if overwrite else "a")
        else:
            self.f = open(path, "w" if overwrite else "a")

    def __del__(self):
        self.f.close()

    def quote(self, value):
        if value is None:
            return '\N'
        if isinstance(value, float):
            return "%.15g" % (value,)
        if isinstance(value, str):
            value = re.sub(r'"', r'\"', value)
            return '"' + value.strip() + '"'
        if isinstance(value, dafBase.DateTime):
            value = value.toString()
            return '"' + value[0:10] + ' ' + value[11:19] + '"'
        return str(value)

    def write(self, *fields):
        print >>self.f, ",".join([self.quote(field) for field in fields])
