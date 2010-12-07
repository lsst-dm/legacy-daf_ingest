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

import optparse
import os
import sys
from textwrap import dedent

import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper

from lsst.datarel.csvFileWriter import CsvFileWriter
from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions

class CsvGenerator(object):
    def __init__(self, root, registry=None, compress=True):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        bf = dafPersist.ButlerFactory(
                mapper=CfhtMapper(root=root, registry=registry))
        self.butler = bf.create()

        self.ampFile = CsvFileWriter("sdqa_Rating_ForScienceAmpExposure.csv",
                                     compress=compress)
        self.ccdFile = CsvFileWriter("sdqa_Rating_ForScienceCcdExposure.csv",
                                     compress=compress)
        self.rawToSnapFile = CsvFileWriter("Raw_Amp_To_Snap_Ccd_Exposure.csv",
                                           compress=compress)
        self.snapToSciFile = CsvFileWriter("Snap_Ccd_To_Science_Ccd_Exposure.csv",
                                           compress=compress)

    def csvAll(self):
        for visit, ccd in self.butler.queryMetadata("raw", "ccd",
                ("visit", "ccd")):
            if self.butler.datasetExists("sdqaCcd", visit=visit, snap=0,
                    ccd=ccd):
                self.toCsv(visit, ccd)
        self.ampFile.flush()
        self.ccdFile.flush()
        self.rawToSnapFile.flush()
        self.snapToSciFile.flush()

    def toCsv(self, visit, ccd):
        sciCcdExposureId = (long(visit) << 6) + ccd

        snapCcdExposureId = sciCcdExposureId
        self.snapToSciFile.write(snapCcdExposureId, 0, sciCcdExposureId)

        prv = self.butler.get("sdqaCcd", visit=visit, ccd=ccd)
        for r in prv.getSdqaRatings():
            self.ccdFile.write(r.getName(), snapCcdExposureId,
                    r.getValue(), r.getErr())

        for amp in xrange(2):
            rawAmpExposureId = (snapCcdExposureId << 1) + amp
            self.rawToSnapFile.write(rawAmpExposureId, amp, snapCcdExposureId)

            prv = self.butler.get("sdqaAmp", visit=visit, ccd=ccd, amp=amp)
            for r in prv.getSdqaRatings():
                self.ampFile.write(r.getName(), rawAmpExposureId,
                        r.getValue(), r.getErr())
        print "Processed visit %d ccd %d" % (visit, ccd)

def dbLoad(sql):
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE sdqa_Rating_ForScienceAmpExposure
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        (@name, ampExposureId, metricValue, metricSigma)
        SET sdqa_metricId = (
                SELECT sdqa_metricId FROM sdqa_Metric
                WHERE metricName = @name), 
            sdqa_thresholdId =  (
                SELECT sdqa_thresholdId FROM sdqa_Threshold
                WHERE sdqa_Threshold.sdqa_metricId = sdqa_metricId
                ORDER BY createdDate DESC LIMIT 1);
        SHOW WARNINGS;
        """ % os.path.abspath("sdqa_Rating_ForScienceAmpExposure.csv")))
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE sdqa_Rating_ForScienceCcdExposure 
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        (@name, ccdExposureId, metricValue, metricSigma)
        SET sdqa_metricId = (
                SELECT sdqa_metricId FROM sdqa_Metric
                WHERE metricName = @name), 
            sdqa_thresholdId =  (
                SELECT sdqa_thresholdId FROM sdqa_Threshold
                WHERE sdqa_Threshold.sdqa_metricId = sdqa_metricId
                ORDER BY createdDate DESC LIMIT 1);
        SHOW WARNINGS;
        """ % os.path.abspath("sdqa_Rating_ForScienceCcdExposure.csv")))
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE Raw_Amp_To_Snap_Ccd_Exposure
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
            rawAmpExposureId,
            amp,
            snapCcdExposureId);
        SHOW WARNINGS;
        """ % os.path.abspath("Raw_Amp_To_Snap_Ccd_Exposure.csv")))

def main():
    usage = dedent("""\
    usage: %prog [options] <root> [<registry>]

    Program which converts CFHT SDQA ratings to CSV files suitable
    for loading into MySQL. If a database name is specified in the options,
    the CSVs are also loaded into that database.

    Make sure to run prepareDb.py before database loads - this instantiates
    the LSST schema in the target database.
    """)
    parser = optparse.OptionParser(usage)
    addDbOptions(parser)
    parser.add_option(
        "-d", "--database", dest="database",
        help="MySQL database to load CSV files into.")
    opts, args = parser.parse_args()
    if len(args) == 2:
        root, registry = args
    elif len(args) == 1:
        root, registry = args[0], None
    load = opts.database != None
    if load :
        if opts.user == None:
            parser.error("No database user name specified and $USER " +
                         "is undefined or empty")
        sql = MysqlExecutor(opts.host, opts.database, opts.user, opts.port)
    c = CsvGenerator(root, registry, not load)
    c.csvAll()
    if load:
        dbLoad(sql)

if __name__ == '__main__':
    main()
