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
import argparse
import os
import subprocess
import sys
from textwrap import dedent
import glob
import re

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
from lsst.obs.sdss import SdssMapper
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage
import lsst.meas.algorithms as measAlg

from lsst.datarel.csvFileWriter import CsvFileWriter
from lsst.datarel.mysqlExecutor import MysqlExecutor
from lsst.datarel.ingest import makeArgumentParser, visitSdssCalexps

if not 'SCISQL_DIR' in os.environ:
    print >>sys.stderr, "Please setup the scisql package and try again"
    sys.exit(1)

scisqlIndex = os.path.join(os.environ['SCISQL_DIR'], 'bin', 'scisql_index')

filterMap = ["u", "g", "r", "i", "z"]

sigmaToFwhm = 2.0*math.sqrt(2.0*math.log(2.0))


class CsvGenerator(object):
    def __init__(self, namespace, compress=True):
        self.namespace = namespace
        self.expFile = CsvFileWriter(
            os.path.join(namespace.outroot, "Science_Ccd_Exposure.csv"),
            compress=compress)
        self.mdFile = CsvFileWriter(
            os.path.join(namespace.outroot, "Science_Ccd_Exposure_Metadata.csv"),
            compress=compress)
        self.polyFile = open(
            os.path.join(namespace.outroot, "Science_Ccd_Exposure_Poly.tsv"), "wb")

    def csvAll(self, namespace, sql=None):
        def _toCsv(butler, path, sciCcdExpId, run, camcol, filter, field):
            self.toCsv(butler, path, sciCcdExpId, run, camcol, filter, field)
        self.mdFile.write("scienceCcdExposureId", "metadataKey", "exposureType",
                          "intValue", "doubleValue", "stringValue")
        visitSdssCalexps(namespace, _toCsv, sql)
        self.expFile.flush()
        self.mdFile.flush()
        self.polyFile.flush()
        self.polyFile.close()

    def toCsv(self, butler, filename, sciCcdExpId, run, camcol, filter, field):
        if False: # os.stat(filename).st_size < (4+2+4)*2048*1489:
            msg = str.format("run {} camcol {} filter {} field {}: too small, possibly corrupt",
                             run, camcol, filter, field) 
            if not self.namespace.strict:
                print >>sys.stderr, "*** Skipping " + msg
                return
            else:
                raise RuntimeError(msg)
        md = afwImage.readMetadata(filename)
        width = md.get('NAXIS1')
        height = md.get('NAXIS2')
        wcs = afwImage.makeWcs(md.deepCopy())
        cen = wcs.pixelToSky(0.5*width - 0.5, 0.5*height - 0.5).toIcrs()
        corner1 = wcs.pixelToSky(-0.5, -0.5).toIcrs()
        corner2 = wcs.pixelToSky(-0.5, height - 0.5).toIcrs()
        corner3 = wcs.pixelToSky(width - 0.5, height - 0.5).toIcrs()
        corner4 = wcs.pixelToSky(width - 0.5, -0.5).toIcrs()
        psf = butler.get("psf", run=run, camcol=camcol, filter=filter, field=field)
        msg = str.format("run {} camcol {} filter {} field {}: PSF missing or corrupt",
                         run, camcol, filter, field)
        noPsf = True
        try:
            noPsf = psf is None
        except:
            pass
        if noPsf:
            if not self.namespace.strict:
                print >>sys.stderr, "*** Skipping " + msg
                return
            else:
                raise RuntimeError(msg)

        attr = measAlg.PsfAttributes(psf, width // 2, height // 2)
        fwhm = attr.computeGaussianWidth() * wcs.pixelScale().asArcseconds() * sigmaToFwhm
        expTime = md.get('EXPTIME') # s
        halfExpTimeNs = long(round(expTime * 500000000.0)) # 0.5 * expTime in ns
        obsStart = dafBase.DateTime(
            dafBase.DateTime(md.get('TIME-MID')).nsecs(dafBase.DateTime.TAI) - halfExpTimeNs,
            dafBase.DateTime.TAI)
        self.expFile.write(
            sciCcdExpId, run, camcol,
            filterMap.index(filter), field, filter,
            cen.getRa().asDegrees(), cen.getDec().asDegrees(),
            md.get('EQUINOX'), md.get('RADESYS'),
            md.get('CTYPE1'), md.get('CTYPE2'),
            md.get('CRPIX1'), md.get('CRPIX2'),
            md.get('CRVAL1'), md.get('CRVAL2'),
            md.get('CD1_1'), md.get('CD1_2'),
            md.get('CD2_1'), md.get('CD2_2'),
            corner1.getRa().asDegrees(), corner1.getDec().asDegrees(),
            corner2.getRa().asDegrees(), corner2.getDec().asDegrees(),
            corner3.getRa().asDegrees(), corner3.getDec().asDegrees(),
            corner4.getRa().asDegrees(), corner4.getDec().asDegrees(),
            obsStart.get(dafBase.DateTime.MJD, dafBase.DateTime.TAI),
            obsStart,
            md.get('TIME-MID'), md.get('EXPTIME'),
            1, 1, 1,
            md.get('FLUXMAG0'), md.get('FLUXMAG0ERR'),
            fwhm)
        for name in md.paramNames():
            if md.typeOf(name) == md.TYPE_Int:
                self.mdFile.write(sciCcdExpId, name, 1, md.getInt(name), None, None)
            elif md.typeOf(name) == md.TYPE_Double:
                self.mdFile.write(sciCcdExpId, name, 1, None, md.getDouble(name), None)
            else:
                self.mdFile.write(sciCcdExpId, name, 1, None, None, str(md.get(name)))
        self.polyFile.write("\t".join([
                str(sciCcdExpId),
                repr(corner1.getRa().asDegrees()), repr(corner1.getDec().asDegrees()),
                repr(corner2.getRa().asDegrees()), repr(corner2.getDec().asDegrees()),
                repr(corner3.getRa().asDegrees()), repr(corner3.getDec().asDegrees()),
                repr(corner4.getRa().asDegrees()), repr(corner4.getDec().asDegrees())]))
        self.polyFile.write("\n")
        print str.format("Processed run {} camcol {} filter {} field {}",
                         run, camcol, filter, field)

def dbLoad(ns, sql):
    subprocess.call([scisqlIndex, "-l", "10",
                     os.path.join(ns.outroot, "Science_Ccd_Exposure_To_Htm10.tsv"),
                     os.path.join(ns.outroot, "Science_Ccd_Exposure_Poly.tsv")])
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' INTO TABLE Science_Ccd_Exposure
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (
            scienceCcdExposureId, run, camcol, filterId, field, filterName,
            ra, decl,
            equinox, raDeSys,
            ctype1, ctype2,
            crpix1, crpix2,
            crval1, crval2,
            cd1_1, cd1_2, cd2_1, cd2_2,
            corner1Ra, corner1Decl,
            corner2Ra, corner2Decl,
            corner3Ra, corner3Decl,
            corner4Ra, corner4Decl,
            taiMjd, obsStart, expMidpt, expTime,
            nCombine, binX, binY,
            fluxMag0, fluxMag0Sigma, fwhm
        ) SET htmId20 = scisql_s2HtmId(ra, decl, 20),
              poly = scisql_s2CPolyToBin(corner1Ra, corner1Decl,
                                         corner2Ra, corner2Decl,
                                         corner3Ra, corner3Decl,
                                         corner4Ra, corner4Decl);
        SHOW WARNINGS;
        """ % os.path.abspath(os.path.join(ns.outroot, "Science_Ccd_Exposure.csv"))))
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' INTO TABLE Science_Ccd_Exposure_Metadata
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        IGNORE 1 LINES (
            scienceCcdExposureId,
            metadataKey,
            exposureType,
            intValue,
            doubleValue,
            stringValue);
        SHOW WARNINGS;
        """ % os.path.abspath(os.path.join(ns.outroot, "Science_Ccd_Exposure_Metadata.csv"))))
    sql.execStmt(dedent("""\
        LOAD DATA LOCAL INFILE '%s' INTO TABLE Science_Ccd_Exposure_To_Htm10 (
            scienceCcdExposureId,
            htmId10);
        SHOW WARNINGS;
        """ % os.path.abspath(os.path.join(ns.outroot, "Science_Ccd_Exposure_To_Htm10.tsv"))))

def main():
    parser = makeArgumentParser(description=
        "Converts processed LSST Sim exposure metadata to CSV files "
        "suitable for loading into MySQL. If a database name is given, "
        "the CSVs are also loaded into that database. Make sure to run "
        "prepareDb.py before database loads - this instantiates the LSST "
        "schema in the target database.")
    ns = parser.parse_args()
    sql = None
    doLoad = ns.database != None
    if doLoad :
        if ns.user == None:
            parser.error("No database user name specified and $USER " +
                         "is undefined or empty")
        sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    c = CsvGenerator(ns, not doLoad)
    c.csvAll(ns, sql)
    if doLoad:
        dbLoad(ns, sql)

if __name__ == '__main__':
    main()

