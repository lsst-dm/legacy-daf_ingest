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
from lsst.obs.lsstSim import LsstSimMapper
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage
import lsst.meas.algorithms as measAlg

from lsst.datarel.csvFileWriter import CsvFileWriter
from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions

rafts = [       "0,1", "0,2", "0,3",
         "1,0", "1,1", "1,2", "1,3", "1,4",
         "2,0", "2,1", "2,2", "2,3", "2,4",
         "3,0", "3,1", "3,2", "3,3", "3,4",
                "4,1", "4,2", "4,3"]

filterMap = ["u", "g", "r", "i", "z", "y"]

class CsvGenerator(object):
    def __init__(self, root, registry=None, compress=True):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        self.mapper = LsstSimMapper(root=root, registry=registry)
        bf = dafPersist.ButlerFactory(mapper=self.mapper)
        self.butler = bf.create()

        self.expFile = CsvFileWriter("Science_Ccd_Exposure.csv",
                                     compress=compress)
        self.mdFile = CsvFileWriter("Science_Ccd_Exposure_Metadata.csv",
                                    compress=compress)

    def csvAll(self):
        for visit, raft, sensor in self.butler.queryMetadata("raw", "sensor",
                ("visit", "raft", "sensor")):
            if self.butler.datasetExists("calexp", visit=visit, raft=raft,
                    sensor=sensor):
                self.toCsv(visit, raft, sensor)
        self.expFile.flush()
        self.mdFile.flush()

    def getFullMetadata(self, datasetType, **keys):
        filename = self.mapper.map(datasetType, keys).getLocations()[0]
        return afwImage.readMetadata(filename)

    def toCsv(self, visit, raft, sensor):
        r1, comma, r2 = raft
        s1, comma, s2 = sensor
        raftNum = rafts.index(raft)
        raftId = int(r1) * 5 + int(r2)
        ccdNum = int(s1) * 3 + int(s2)
        sciCcdExposureId = (long(visit) << 9) + raftId * 10 + ccdNum

        md = self.getFullMetadata("calexp",
                visit=visit, raft=raft, sensor=sensor)
        width = md.get('NAXIS1')
        height = md.get('NAXIS2')
        wcs = afwImage.makeWcs(md.deepCopy())
        llc = wcs.pixelToSky(0, 0).toIcrs()
        ulc = wcs.pixelToSky(0, height - 1).toIcrs()
        urc = wcs.pixelToSky(width - 1, height - 1).toIcrs()
        lrc = wcs.pixelToSky(width - 1, 0).toIcrs()
        psf = self.butler.get("psf", visit=visit, raft=raft, sensor=sensor)
        attr = measAlg.PsfAttributes(psf, width // 2, height // 2)
        fwhm = attr.computeGaussianWidth()
        obsStart = dafBase.DateTime(md.get('MJD-OBS'), dafBase.DateTime.MJD,
                dafBase.DateTime.UTC)
        self.expFile.write(sciCcdExposureId, visit, raftNum, ccdNum,
                filterMap.index(md.get('FILTER').strip()),
                md.get('RA_DEG'), md.get('DEC_DEG'),
                md.get('EQUINOX'), md.get('RADESYS'),
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
                obsStart,
                md.get('TIME-MID'), md.get('EXPTIME'),
                1, 1, 1,
                md.get('RDNOISE'), md.get('SATURATE'), md.get('GAINEFF'),
                md.get('FLUXMAG0'), md.get('FLUXMAG0ERR'),
                fwhm)
        for name in md.paramNames():
            if md.typeOf(name) == md.TYPE_Int:
                self.mdFile.write(sciCcdExposureId, name, 1,
                        md.getInt(name), None, None)
            elif md.typeOf(name) == md.TYPE_Double:
                self.mdFile.write(sciCcdExposureId, name, 1,
                        None, md.getDouble(name), None)
            else:
                self.mdFile.write(sciCcdExposureId, name, 1,
                        None, None, str(md.get(name)))
        print "Processed visit %d raft %s sensor %s" % (visit, raft, sensor)

def dbLoad(sql):
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE Science_Ccd_Exposure
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (
            scienceCcdExposureId, visit, raft, ccd, filterId,
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
            nCombine, binX, binY,
            readNoise, saturationLimit, gainEff,
            fluxMag0, fluxMag0Sigma, fwhm);
        SHOW WARNINGS;
        """ % os.path.abspath("Science_Ccd_Exposure.csv")))
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE Science_Ccd_Exposure_Metadata
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
            scienceCcdExposureId,
            metadataKey,
            exposureType,
            intValue,
            doubleValue,
            stringValue);
        SHOW WARNINGS;
        """ % os.path.abspath("Science_Ccd_Exposure_Metadata.csv")))

def main():
    usage = dedent("""\
    usage: %prog [options] <root> [<registry>]

    Program which converts processed LSST Sim exposure metadata to CSV files
    suitable for loading into MySQL. If a database name is specified in the
    options, the CSVs are also loaded into that database.

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

