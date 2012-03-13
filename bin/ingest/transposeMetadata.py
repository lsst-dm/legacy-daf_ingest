#! /usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#
from __future__ import with_statement
from contextlib import closing
import getpass
import argparse
import MySQLdb as sql
from lsst.daf.persistence import DbAuth

from lsst.datarel.mysqlExecutor import addDbOptions

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
        createStmt = "CREATE TABLE %s (\n    %s BIGINT NOT NULL PRIMARY KEY" % (self.name, self.idCol)
        cols = ",\n".join([c.getColumnSpec() for c in self.columns if c.constVal == None])
        if len(cols) > 0:
            createStmt += ",\n"
            createStmt += cols
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
                    (self.name, metadataTable, self.idCol, self.idCol,c.name, c.getDbName(), c.type))
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

def run(host, port, user, passwd, database,
        metadataTable, idCol, outputTable,
        skipCols=set(), compress=True):
    kw = dict()
    if host is not None:
        kw['host'] = host
    if port is not None:
        kw['port'] = port
    if user is not None:
        kw['user'] = user
    if passwd is not None:
        kw['passwd'] = passwd
    if database is not None:
        kw['db'] = database
    with closing(sql.connect(**kw)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT COUNT(*) FROM " + metadataTable)
            nrows = cursor.fetchall()[0][0]
            if nrows == 0:
                return
            columns = getColumns(cursor, metadataTable, skipCols)
            dbnames = set()
            for c in columns:
                if c.getDbName() in dbnames:
                    raise RuntimeError(
                        "Column %s renamed to %s conflicts with another column!" %
                        (c.name, c.getDbName()))
                dbnames.add(c.getDbName())
                c.computeAttributes(cursor, metadataTable, compress)
            table = OutputTable(outputTable, idCol, columns)
            table.create(cursor, metadataTable)
            table.populate(cursor, metadataTable)

def main():
    # Setup command line options
    parser = argparse.ArgumentParser(description=
        "Program which transposes a key-value table into a table where each key is"
        "mapped to a column.")
    addDbOptions(parser)
    parser.add_argument(
        "-s", "--skip-keys", dest="skipKeys",
        help="Comma separated list of metadata keys to omit in the output table")
    parser.add_argument(
        "-c", "--compress", dest="compress", action="store_true",
        help="Lift keys with constant values into a view")
    parser.add_argument(
        "database", help="Name of database containing metadata table to transpose")
    parser.add_argument(
        "metadataTable", help="Name of metadata table to transpose")
    parser.add_argument(
        "idCol", help="Primary key column name for metadata table")
    parser.add_argument(
        "outputTable", help="Name of output table to create") 
    ns = parser.parse_args()
    db, metadataTable, idCol, outputTable = args
    if DbAuth.available(ns.host, str(ns.port)):
        ns.user = DbAuth.username(ns.host, str(ns.port))
        passwd = DbAuth.password(ns.host, str(ns.port))
    elif os.path.exists(os.path.join(os.environ["HOME"], ".mysql.cnf")):
        passwd = None
    else:
        passwd = getpass.getpass("%s's MySQL password: " % ns.user)
    skipCols = set()
    if opts.skipKeys != None:
        skipCols = set(map(lambda x: x.strip(), opts.skipKeys.split(",")))
    run(ns.host, ns.port, ns.user, passwd, db, metadataTable,
        idCol, outputTable, skipCols, ns.compress)
 
if __name__ == "__main__":
    main()

