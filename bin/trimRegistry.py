#!/usr/bin/env python

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

"""trimRegistry.py [-o OUTPUTREGISTRY] INPUTREGISTRY INPUTDATA

This command trims an input registry INPUTREGISTRY to contain only the rows
relevant for the CCDs specified by the INPUTDATA file.  It does this by
copying the input registry to the output registry (which defaults to the name
"newreg.sqlite3" and must not yet exist), selecting the appropriate rows into
new tables, dropping the old tables, renaming the new tables, and vacuuming
the entire file to compact it.  It works on both ImSim and CFHTLS registries,
as it only relies on the visit and id columns that are common to both.

The INPUTDATA file must be formatted for the job office.  It starts with an
">intids" line specifying which dataset identifier values are integers (as
opposed to strings).  A list of dataset identifiers is then given, each of
which consists of key/value pairs.  The keys must be consistent from dataset
to dataset, although they can be reordered.
"""

from __future__ import with_statement
from optparse import OptionParser
import os
import shutil
try:
    import sqlite3
except ImportError:
    # try external pysqlite package; deprecated
    import sqlite as sqlite3

def rowGenerator(inputData):
    """Interpret the job office text file specified by the inputData pathname,
    yielding the names and (1-based) locations of the keys (with negative
    locations flagging integer fields) and then each dataset identifier in the
    file with values in order."""

    intIds = []
    cols = {}
    rowNum = 0
    with open(inputData, "r") as f:
        for l in f:
            words = l.split()

            if words[0] == ">intids":
                intIds = words[1:]
                continue

            rowNum += 1
            if rowNum > 1:
                row = [None for _ in xrange(len(cols))]
            else:
                row = []
            for i in xrange(1, len(words)):
                pos = words[i].index('=')
                key = words[i][0:pos]
                val = words[i][pos + 1:]
                if rowNum <= 1:
                    if key in cols:
                        raise RuntimeError, "Duplicate key: %s" % (key,)
                    if key in intIds:
                        cols[key] = -i
                    else:
                        cols[key] = i
                    row.append(None)
                pos = cols[key]
                if pos < 0:
                    row[(-pos)-1] = int(val)
                else:
                    row[pos-1] = val
            if rowNum == 1:
                yield cols
            yield row

def main(inputRegistry, outputRegistry, inputData):
    """Trim the SQLite3 file specified by the inputRegistry pathname,
    producing the output SQLite3 file specified by the outputRegistry
    pathname.  Use the text file (suitable for the job office) specified by
    the inputData pathname to select rows out of the registry."""

    if not os.path.exists(inputRegistry):
        raise RuntimeError, "Registry not found"
    if os.path.exists(outputRegistry):
        raise RuntimeError, "Output registry already exists"
    shutil.copyfile(inputRegistry, outputRegistry)
    with sqlite3.connect(outputRegistry) as db:
        gen = rowGenerator(inputData)
        cols = gen.next()
        schema = [None for _ in xrange(len(cols))]
        for k, v in cols.iteritems():
            if v < 0:
                schema[(-v)-1] = "%s INT" % (k,)
            else:
                schema[v-1] = "%s TEXT" % (k,)
        cmd = "CREATE TABLE selector (" + ", ".join(schema) + ")"
        print cmd
        db.execute(cmd)
        cmd = "INSERT INTO selector VALUES (" + ", ".join(["?" for _ in
                xrange(len(cols))]) + ")"
        print cmd
        db.executemany(cmd, gen)
        cmd = "CREATE TABLE new_raw AS " + \
                "SELECT raw.* FROM raw, selector WHERE " + " AND ".join(
                        ["raw.%s = selector.%s" % (col, col)
                        for col in cols.iterkeys()])
        print cmd
        db.execute(cmd)
        cmd = "CREATE TABLE new_raw_skyTile AS " + \
                "SELECT * from raw_skyTile " + \
                "WHERE id IN (SELECT id FROM new_raw)"
        print cmd
        db.execute(cmd)
        cmd = "CREATE TABLE new_raw_visit AS " + \
                "SELECT * from raw_visit " + \
                "WHERE visit IN (SELECT DISTINCT visit FROM new_raw)"
        print cmd
        db.execute(cmd)
        cmd = "CREATE UNIQUE INDEX pk_id ON new_raw (id)"
        print cmd
        db.execute(cmd)
        cmd = "CREATE UNIQUE INDEX pk_visit ON new_raw_visit (visit)"
        print cmd
        db.execute(cmd)
        db.commit()

        cmd = "DROP TABLE raw"
        print cmd
        db.execute(cmd)
        cmd = "DROP TABLE raw_skyTile"
        print cmd
        db.execute(cmd)
        cmd = "DROP TABLE raw_visit"
        print cmd
        db.execute(cmd)
        cmd = "DROP TABLE selector"
        print cmd
        db.execute(cmd)
        cmd = "ALTER TABLE new_raw RENAME TO raw"
        print cmd
        db.execute(cmd)
        cmd = "ALTER TABLE new_raw_skyTile RENAME TO raw_skyTile"
        print cmd
        db.execute(cmd)
        cmd = "ALTER TABLE new_raw_visit RENAME TO raw_visit"
        print cmd
        db.execute(cmd)
        cmd = "VACUUM"
        print cmd
        db.execute(cmd)
        db.commit()

if __name__ == "__main__":
    parser = OptionParser(usage="""\
usage: %prog [options] INPUTREGISTRY INPUTDATA

INPUTDATA is an input file for the job office.""")
    parser.add_option("-o", "--output", dest="output",
            default="newreg.sqlite3",
            help="output registry (default=newreg.sqlite3)")
    (options, args) = parser.parse_args()
    if len(args) < 2:
        parser.error("Missing input registry or input data file")
    inputData = args[1]
    if not os.path.exists(inputData):
        raise RuntimeError, "Missing input data file: %s" % (inputData,)
    main(args[0], options.output, inputData)
