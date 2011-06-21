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

import math
import optparse
import os
import sys
from textwrap import dedent
import glob
import re

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

sigmaToFwhm = 2.0*math.sqrt(2.0*math.log(2.0))

class CsvGenerator(object):
    def __init__(self, root, registry=None, compress=True):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        self.root = root
        self.mapper = LsstSimMapper(root=root, registry=registry)
        bf = dafPersist.ButlerFactory(mapper=self.mapper)
        self.butler = bf.create()

        self.expFile = CsvFileWriter("Science_Ccd_Exposure.csv",
                                     compress=compress)
        self.mdFile = CsvFileWriter("Science_Ccd_Exposure_Metadata.csv",
                                    compress=compress)

    def getNext(self, datasetType):
        # for visit, raft, sensor in self.butler.queryMetadata("raw", "sensor",
        #         ("visit", "raft", "sensor")):
        #     if self.butler.datasetExists("calexp", visit=visit, raft=raft,
        #             sensor=sensor):
        #         yield (visit, raft, sensor)

        for f in glob.glob(os.path.join(self.root, "calexp", "v*-f*", "R*",
            "S*.fits")):
            m = re.search(r'calexp/v(\d+)-f.*/R(\d\d)/S(\d\d).fits', f)
            assert m
            visit = int(m.group(1))
            raft = ",".join(m.group(2))
            sensor = ",".join(m.group(3))
            yield (visit, raft, sensor)

    def csvAll(self):
        for visit, raft, sensor in self.getNext("calexp"):
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

        filename = self.mapper.map("calexp",
                dict(visit=visit, raft=raft, sensor=sensor)).getLocations()[0]
        if os.stat(filename).st_size < (4+2+4)*4000*4000:
            print >>sys.stderr, "*** Too small (possibly corrupt), skipped: visit %d raft %s sensor %s" % (visit, raft, sensor)
            return

        md = afwImage.readMetadata(filename)
        width = md.get('NAXIS1')
        height = md.get('NAXIS2')
        wcs = afwImage.makeWcs(md.deepCopy())
        cen = wcs.pixelToSky(0.5*width - 0.5, 0.5*height - 0.5).toIcrs()
        llc = wcs.pixelToSky(-0.5, -0.5).toIcrs()
        ulc = wcs.pixelToSky(-0.5, height - 0.5).toIcrs()
        urc = wcs.pixelToSky(width - 0.5, height - 0.5).toIcrs()
        lrc = wcs.pixelToSky(width - 0.5, -0.5).toIcrs()
        psf = self.butler.get("psf", visit=visit, raft=raft, sensor=sensor)
        try:
            if psf is None:
                print >>sys.stderr, "*** PSF missing or corrupt, skipped: visit %d raft %s sensor %s" % (visit, raft, sensor)
                return
        except:
            print >>sys.stderr, "*** PSF corrupt or missing, skipped: visit %d raft %s sensor %s" % (visit, raft, sensor)
            return

        attr = measAlg.PsfAttributes(psf, width // 2, height // 2)
        fwhm = attr.computeGaussianWidth() * wcs.pixelScale() * sigmaToFwhm
        obsStart = dafBase.DateTime(md.get('MJD-OBS'), dafBase.DateTime.MJD,
                dafBase.DateTime.UTC)
        filterName = md.get('FILTER').strip()
        self.expFile.write(sciCcdExposureId, visit, raftNum, raft,
                ccdNum, sensor,
                filterMap.index(filterName), filterName,
                cen.getRa(afwCoord.DEGREES), cen.getDec(afwCoord.DEGREES),
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
            scienceCcdExposureId, visit, raft, raftName, ccd, ccdName,
            filterId, filterName,
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

