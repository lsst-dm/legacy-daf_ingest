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

import optparse
import os
import string
from textwrap import dedent

from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions

def main():
    usage = dedent("""\
    usage: %prog [options] [-t <type>] <database> 

    Program which "links" an LSST per-run database into the well-known
    database name buildbot_weekly_latest by creating views for each table.

    <type>:       Type of stack; either 'tags' or 'trunk'; Default is 'tags'.
    <database>:   Name of database to "link" from
    """)
    parser = optparse.OptionParser(usage)
    parser.add_option("-t","--type",default="tags",
        help="type of stack which generated DB; either 'tags' or 'trunk'; default 'tags'")
    addDbOptions(parser)
    opts, args = parser.parse_args()
    print opts, args
    if len(args) != 1:
        parser.error("A single argument (database name) must be supplied.")
    database = args[0]
    if opts.user == None:
        parser.error("No database user name specified and $USER is undefined or empty")
    if opts.type != "tags" and opts.type != "trunk":
        parser.error("Only 'trunk' or 'tags' may be specified for type.")
    viewName = "buildbot_weekly_latest_" + opts.type
    print viewName

    sql = MysqlExecutor(opts.host, viewName, opts.user, opts.port)
    for table in (
            "AmpMap", "CcdMap", "Filter", "LeapSeconds", "Logs",
            "NonVarObject", "Object", "ObjectType", "RaftMap",
            "Raw_Amp_Exposure", "Raw_Amp_Exposure_Metadata",
            "Raw_Amp_Exposure_To_Htm11", "Raw_Amp_To_Science_Ccd_Exposure",
            "Raw_Amp_To_Snap_Ccd_Exposure", "RefObjMatch", "RefSrcMatch",
            "Science_Ccd_Exposure", "Science_Ccd_Exposure_Metadata",
            "Science_Ccd_Exposure_To_Htm10", "SimRefObject",
            "Snap_Ccd_To_Science_Ccd_Exposure", "Source", "Visit",
            "ZZZ_Db_Description", "_tmpl_InMemoryObject", "sdqa_ImageStatus",
            "sdqa_Metric", "sdqa_Rating_ForScienceAmpExposure",
            "sdqa_Rating_ForScienceCcdExposure", "sdqa_Threshold"):
        sql.execStmt("""CREATE OR REPLACE
            SQL SECURITY INVOKER
            VIEW %s AS
            SELECT * FROM %s.%s;""" % (table, database, table))
    sql.execStmt(
            "UPDATE ZZZ_View_Description SET src='%s';" % (database,))

if __name__ == "__main__":
    main()
