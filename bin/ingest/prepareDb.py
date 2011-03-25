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
import optparse
import os
from textwrap import dedent

from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions

loadTables = ["Source",
              "BadSource",
              "Object",
              "SimRefObject",
              "RefObjMatch",
              "Science_Ccd_Exposure",
              "Science_Ccd_Exposure_Metadata",
              "Raw_Amp_Exposure",
              "Raw_Amp_Exposure_Metadata",
              "Raw_Amp_To_Science_CcdExposure",
              "sdqa_Rating_ForScienceCcdExposure",
              "sdqa_Rating_ForSnapCcdExposure",
              "Raw_Amp_To_Snap_Ccd_Exposure",
              "Snap_Ccd_To_Science_Ccd_Exposure",
             ]

def main():
    usage = dedent("""\
    usage: %prog [options] <database>

    Program which creates an LSST run database and instantiates the LSST
    schema therein. Indexes on tables which will be loaded by the various
    datarel ingest scripts are disabled. Once loading has finished, the
    finishDb.py script should be run to re-enable them.

    <database>:   Name of database to create and instantiate the LSST schema in.
    """)
    parser = optparse.OptionParser(usage)
    addDbOptions(parser)
    opts, args = parser.parse_args()
    if len(args) != 1:
        parser.error("A single argument (database name) must be supplied.")
    database = args[0]
    if opts.user == None:
        parser.error("No database user name specified and $USER is undefined or empty")
    if 'CAT_DIR' not in os.environ or len(os.environ['CAT_DIR']) == 0:
        parser.error("$CAT_DIR is undefined or empty - please setup the cat " +
                     "package and try again.")
    catDir = os.environ['CAT_DIR']
    sql = MysqlExecutor(opts.host, database, opts.user, opts.port)
    sql.createDb(database)
    sql.execScript(os.path.join(catDir, 'sql', 'lsstSchema4mysqlPT1_2.sql'))
    sql.execScript(os.path.join(catDir, 'sql', 'setup_perRunTables.sql'))
    sql.execScript(os.path.join(catDir, 'sql', 'setup_storedFunctions.sql'))
    sql.execScript(os.path.join(catDir, 'sql', 'setup_sdqa.sql'))
    # Disable indexes on tables for faster loading
    for table in loadTables:
        sql.execStmt("ALTER TABLE %s DISABLE KEYS;" % table)

if __name__ == "__main__":
    main()

