#! /usr/bin/env python

import getpass
import optparse
import MySQLdb as sql
from textwrap import dedent

renames = { 'DEC': 'DECL',
            'OUTFILE': 'OUTFILE_'
          }

class Column(object):
    def __init__(self, name, type):
        self.name = name
        self.type = type 
        self.dbtype = None
        self.notNull = False
        self.minVal = None
        self.maxVal = None
        self.constVal = None

    def computeAttributes(self, cursor, metadataTable, compress):
        print "Computing attributes for metadataKey: " + self.name
        cursor.execute(
            "SELECT count(*) FROM %s WHERE metadataKey='%s' AND %sValue IS NULL;" %
            (metadataTable, self.name, self.type))
        self.notNull = (cursor.fetchone()[0] == 0)
        if self.type == "string":
            cursor.execute("""SELECT MIN(LENGTH(stringValue)), MAX(LENGTH(stringValue))
                FROM %s WHERE metadataKey='%s';""" % (metadataTable, self.name))
            self.minVal, self.maxVal = cursor.fetchone()
            self.dbtype = "CHAR(%d)" % self.maxVal
            # Use VARCHAR? Access and updates will be slower though...
            if self.notNull and self.minVal == self.maxVal and compress:
                # Check whether the column value is constant
                cursor.execute(
                    "SELECT %sValue FROM %s WHERE metadataKey='%s' LIMIT 1;" %
                    (self.type, metadataTable, self.name))
                val = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT %sValue FROM %s WHERE metadataKey='%s' AND %sValue != '%s' LIMIT 1;" %
                    (self.type, metadataTable, self.name, self.type, val))
                rows = cursor.fetchone()
                if rows == None or len(rows) == 0:
                    self.constVal = val
        else:
            cursor.execute("SELECT MIN(%sValue), MAX(%sValue) FROM %s WHERE metadataKey='%s';" %
                (self.type, self.type, metadataTable, self.name))
            self.minVal, self.maxVal = cursor.fetchone()
            if self.type == "int":
               if self.minVal >= -128 and self.maxVal <= 127:
                   self.dbtype = "TINYINT"
               elif self.minVal >= -23768 and self.maxVal <= 32767:
                   self.dbtype = "SMALLINT"
               else:
                   self.dbtype = "INTEGER"
            else:
               self.dbtype = "DOUBLE";
            if self.notNull and self.minVal == self.maxVal and compress:
                self.constVal = self.maxVal

    def getDbName(self):
        if self.name in renames:
            return renames[self.name]
        return self.name.replace("-","_")

    def getColumnSpec(self):
        constraint = ""
        default = "DEFAULT "
        if self.notNull:
            constraint = "NOT NULL"
            if self.type == "string":
                default += "''"
            else:
                default += "0"
        else:
            default += "NULL"
        return " ".join(["   ", self.getDbName(), self.dbtype, constraint, default])


class OutputTable(object):
    def __init__(self, name, idCol, columns):
        self.name = name
        self.idCol = idCol
        self.columns = columns

    def create(self, cursor, metadataTable):
        createStmt = "CREATE TABLE %s (\n    %s BIGINT NOT NULL PRIMARY KEY,\n" % (self.name, self.idCol)
        createStmt += ",\n".join([c.getColumnSpec() for c in self.columns if c.constVal == None])
        createStmt += "\n);"
        print createStmt
        cursor.execute(createStmt)
        cursor.fetchall()
        if any(c.constVal != None for c in self.columns):
            # Create a VIEW which provides columns for metadataKeys with constant values
            viewStmt = "CREATE VIEW %s_View AS SELECT *" % self.name
            for c in self.columns:
                if c.constVal != None:
                    viewStmt += ",\n    "
                    if c.type == "string":
                        viewStmt += "'%s' AS %s" % (c.constVal, c.getDbName())
                    elif c.type == "double":
                        viewStmt += "%s AS %s" % (repr(c.constVal), c.getDbName())
                    else:
                        viewStmt += "%s AS %s" % (str(c.constVal), c.getDbName())
            viewStmt += "\nFROM %s;" % metadataTable
            print viewStmt
            cursor.execute(viewStmt)
            cursor.fetchall()

    def populate(self, cursor, metadataTable):
        print "Storing " + self.idCol + " values"
        cursor.execute("INSERT INTO %s (%s) SELECT DISTINCT %s FROM %s;" %
            (self.name, self.idCol, self.idCol, metadataTable))
        cursor.fetchall()
        for c in self.columns:
            if c.constVal == None:
                print "Storing values for " + c.name
                cursor.execute(
                    """UPDATE %s AS a INNER JOIN %s AS b
                       ON (a.%s = b.%s AND b.metadataKey = '%s')
                       SET a.%s = b.%sValue;""" %
                    (self.name, metadataTable, self.idCol, self.idCol, c.name, c.getDbName(), c.type))
                cursor.fetchall()


def getColumns(cursor, metadataTable, skipCols):
    getColsStmt = """
        SELECT DISTINCT metadataKey, IF(stringValue IS NOT NULL, "string",
            IF(intValue IS NOT NULL, "int", "double")) AS type
        FROM %s WHERE stringValue IS NOT NULL OR intValue IS NOT NULL OR
            doubleValue IS NOT NULL;""" % metadataTable
    columns = []
    names = set()
    cursor.execute(getColsStmt)
    for row in cursor.fetchall():
        name, type = row
        if name in skipCols:
            continue
        if name in names:
            raise RuntimeError("Metadata key %s has inconsistent type!" % name)
        names.add(name)
        columns.append(Column(*row))
    return columns

def hostPort(sv):
    hp = sv.split(':')
    if len(hp) > 1:
        return (hp[0], int(hp[1]))
    else:
        return (hp[0], None)

def main():
    # Setup command line options
    usage = dedent("""\
    usage: %prog [options] <database> <metadataTable> <idCol> <outputTable>

    Program which transposes a key-value table into a table where each key is 
    mapped to a column.

    <database>:       Name of database to operate in.
    <metadataTable>:  Name of key value table to transpose
    <idCol>:          ID (primary key) column of <metadataTable>.
    <outputTable>:    Name of output table to create.
    """)
    parser = optparse.OptionParser(usage)
    parser.add_option(
        "-u", "--user", dest="user", default="serge",
        help="Database user name to use when connecting to MySQL.")
    parser.add_option(
        "-S", "--server", dest="server", default="lsst10.ncsa.uiuc.edu:3306",
        help="host:port of MySQL server to connect to; defaults to %default")
    parser.add_option(
        "-s", "--skip-keys", dest="skipKeys",
        help="Comma separated list of metadata keys to omit in the output table")
    parser.add_option(
        "-c", "--compress", dest="compress", action="store_true",
        help="Lift keys with constant values into a view")
    opts, args = parser.parse_args()
    if len(args) != 4:
        parser.error("Invalid number of arguments")
    db, metadataTable, idCol, outputTable = args
    passwd = getpass.getpass()
    host, port = hostPort(opts.server)
    conn = sql.connect(host=host, port=port, user=opts.user, passwd=passwd, db=db)
    cursor = conn.cursor()
    skipCols = set() 
    if opts.skipKeys != None:
        skipCols = set(map(lambda x: x.strip(), opts.skipKeys.split(",")))
    columns = getColumns(cursor, metadataTable, skipCols)
    dbnames = set()
    for c in columns:
        if c.getDbName() in dbnames:
            raise RuntimeError("Column %s renamed to %s conflicts with another column!" %
                               (c.name, c.getDbName()))
        dbnames.add(c.getDbName())
        c.computeAttributes(cursor, metadataTable, opts.compress)
    table = OutputTable(outputTable, idCol, columns)
    table.create(cursor, metadataTable)
    table.populate(cursor, metadataTable)
 
if __name__ == "__main__":
    main()
