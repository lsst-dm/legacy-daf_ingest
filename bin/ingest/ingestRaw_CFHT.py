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

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage

from lsst.datarel.csvFileWriter import CsvFileWriter
from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions

filterMap = ["u.MP9301", "g.MP9401", "r.MP9601", "i.MP9701", "z.MP9801",
        "i2.MP9702"]

class CsvGenerator(object):
    def __init__(self, root, registry=None, compress=True):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        self.mapper = CfhtMapper(root=root, registry=registry)
        bf = dafPersist.ButlerFactory(mapper=self.mapper)
        self.butler = bf.create()

        self.expFile = CsvFileWriter("Raw_Amp_Exposure.csv",
                                     compress=compress)
        self.mdFile = CsvFileWriter("Raw_Amp_Exposure_Metadata.csv",
                                    compress=compress)
        self.rToSFile = CsvFileWriter("Raw_Amp_To_Science_Ccd_Exposure.csv",
                                      compress=compress)

    def csvAll(self):
        for visit, ccd in self.butler.queryMetadata("raw", "ccd",
                ("visit", "ccd")):
            if self.butler.datasetExists("raw", visit=visit, ccd=ccd, amp=0):
                self.toCsv(visit, ccd)
        self.expFile.flush()
        self.mdFile.flush()
        self.rToSFile.flush()

    def getFullMetadata(self, datasetType, **keys):
        filename = self.mapper.map(datasetType, keys).getLocations()[0]
        return afwImage.readMetadata(filename)

    def toCsv(self, visit, ccd):
        sciCcdExposureId = (long(visit) << 6) + ccd

        for amp in (0, 1):
            rawAmpExposureId = (sciCcdExposureId << 1) + amp

            try:
                md = self.getFullMetadata("raw", visit=visit, ccd=ccd, amp=amp)
            except:
                print ("*** Unable to read metadata for " + \
                        "visit %d ccd %d amp %d") % (visit, ccd, amp)
                continue

            self.rToSFile.write(rawAmpExposureId, sciCcdExposureId, 0, amp)

            width = md.get('NAXIS1')
            height = md.get('NAXIS2')
            wcs = afwImage.makeWcs(md.deepCopy())

            cen = wcs.pixelToSky(0.5*width - 0.5, 0.5*height - 0.5)
            llc = wcs.pixelToSky(-0.5, -0.5).toIcrs()
            ulc = wcs.pixelToSky(-0.5, height - 0.5).toIcrs()
            urc = wcs.pixelToSky(width - 0.5, height - 0.5).toIcrs()
            lrc = wcs.pixelToSky(width - 0.5, -0.5).toIcrs()
            obsStart = dafBase.DateTime(md.get('MJD-OBS'),
                    dafBase.DateTime.MJD, dafBase.DateTime.UTC)
            expTime = md.get('EXPTIME')
            obsMidpoint = dafBase.DateTime(obsStart.nsecs() +
                    long(expTime * 1000000000L / 2))
            self.expFile.write(rawAmpExposureId,
                    visit, 0, 0, ccd, amp,
                    filterMap.index(md.get('FILTER').strip()),
                    cen.getRa(afwCoord.DEGREES), cen.getDec(afwCoord.DEGREES),
                    md.get('EQUINOX'),
                    md.get('RADECSYS'), # note wrong name
                    md.get('CTYPE1'), md.get('CTYPE2'),
                    md.get('CRPIX1'), md.get('CRPIX2'),
                    md.get('CRVAL1'), md.get('CRVAL2'),
                    md.get('CD1_1'), md.get('CD1_2'),
                    md.get('CD2_1'), md.get('CD2_2'),
                    llc.getRa(afwCoord.DEGREES), llc.getDec(afwCoord.DEGREES),
                    ulc.getRa(afwCoord.DEGREES), ulc.getDec(afwCoord.DEGREES),
                    urc.getRa(afwCoord.DEGREES), urc.getDec(afwCoord.DEGREES),
                    lrc.getRa(afwCoord.DEGREES), lrc.getDec(afwCoord.DEGREES),
                    obsStart.get(dafBase.DateTime.MJD, dafBase.DateTime.TAI),
                    obsStart, obsMidpoint.toString(), expTime,
                    md.get('AIRMASS'), md.get('DARKTIME'),
                    None) # ZENITH is missing
            for name in md.paramNames():
                if md.typeOf(name) == md.TYPE_Int:
                    self.mdFile.write(rawAmpExposureId, 1, name,
                            md.getInt(name), None, None)
                elif md.typeOf(name) == md.TYPE_Double:
                    self.mdFile.write(rawAmpExposureId, 1, name,
                            None, md.getDouble(name), None)
                else:
                    self.mdFile.write(rawAmpExposureId, 1, name,
                            None, None, str(md.get(name)))

        print "Processed visit %d ccd %d" % (visit, ccd)

def dbLoad(sql):
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE Raw_Amp_Exposure
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
            rawAmpExposureId, visit, snap, raft, ccd, amp, filterId,
            ra, decl,
            equinox, raDeSys,
            ctype1, ctype2,
            crpix1, crpix2,
            crval1, crval2,
            cd1_1, cd1_2, cd2_1, cd2_2,
            llcRa, llcDecl,
            ulcRa, ulcDecl,
            urcRa, urcDecl,
            lrcRa, lrcDecl,
            taiMjd, obsStart, expMidpt, expTime,
            airmass, darkTime, zd);
        SHOW WARNINGS;
        """ % os.path.abspath("Raw_Amp_Exposure.csv")))
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE Raw_Amp_Exposure_Metadata
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
            rawAmpExposureId,
            exposureType,
            metadataKey,
            intValue,
            doubleValue,
            stringValue);
        SHOW WARNINGS;
        """ % os.path.abspath("Raw_Amp_Exposure_Metadata.csv")))
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE Raw_Amp_To_Science_Ccd_Exposure
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
            rawAmpExposureId,
            scienceCcdExposureId,
            snap,
            amp);
        SHOW WARNINGS;
        """ % os.path.abspath("Raw_Amp_To_Science_Ccd_Exposure.csv")))

def main():
    usage = dedent("""\
    usage: %prog [options] <root> [<registry>]

    Program which converts raw CFHT exposure metadata to CSV files suitable
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

