#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2012 LSST Corporation.
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

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersistence
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage
import lsst.afw.geom as afwGeom
import lsst.meas.algorithms as measAlg

from lsst.datarel.csvFileWriter import CsvFileWriter
from lsst.datarel.mysqlExecutor import MysqlExecutor
from lsst.datarel.ingest import makeArgumentParser, makeRules
from lsst.datarel.datasetScanner import getMapperClass, DatasetScanner

# Hack to be able to read multiShapelet configs
try:
    import lsst.meas.extensions.multiShapelet
except:
    pass

if not 'SCISQL_DIR' in os.environ:
    print >>sys.stderr, 'Please setup the scisql package and try again'
    sys.exit(1)

scisqlIndex = os.path.join(os.environ['SCISQL_DIR'], 'bin', 'scisql_index')
sigmaToFwhm = 2.0*math.sqrt(2.0*math.log(2.0))
# 4/2/4 bytes for intensity/mask/variance
bytesPerPixel = 4 + 2 + 4

# Camera specific minimum file sizes for calexp FITS files.
# Obtained by multiplying the expected calexp dimensions by the total number
# of bytes required per pixel. 
minExposureSize = {
    'lsstsim': bytesPerPixel*4000*4000,
    'sdss': bytesPerPixel*2048*1361,
    'cfht': bytesPerPixel*1*1, # TODO: what dimensions are appropriate here?
}


class CsvGenerator(object):
    def __init__(self, namespace, compress=True):
        self.namespace = namespace
        self.camera = namespace.camera
        self.expFile = CsvFileWriter(
            os.path.join(namespace.outroot, 'Science_Ccd_Exposure.csv'),
            compress=compress)
        self.mdFile = CsvFileWriter(
            os.path.join(namespace.outroot, 'Science_Ccd_Exposure_Metadata.csv'),
            compress=compress)
        self.polyFile = open(
            os.path.join(namespace.outroot, 'Science_Ccd_Exposure_Poly.tsv'), 'wb')

    def csvAll(self, sql=None):
        """Extract/compute metadata for all single frame exposures matching
        at least one data ID specification, and store it in CSV files.
        """
        # Writer column name header line for calexp metadata CSV
        self.mdFile.write('scienceCcdExposureId', 'metadataKey', 'exposureType',
                          'intValue', 'doubleValue', 'stringValue')
        conn = sql.getConn() if sql else None
        cursor = conn.cursor() if conn else None
        # Loop over input roots
        for root in self.namespace.inroot:
            print 'Ingesting from ' + root
            if hasattr(self.namespace, 'registry'):
                registry = self.namespace.registry
            else:
                registry = os.path.join(root, 'registry.sqlite3')
            cls = getMapperClass(self.camera)
            cameraMapper = cls(root=root, registry=registry)
            butler = dafPersistence.ButlerFactory(mapper=cameraMapper).create()
            scanner = DatasetScanner(dataset='calexp',
                                     camera=self.camera,
                                     cameraMapper=cameraMapper)
            # scan the root for matching calexps
            for path, dataId in scanner.walk(root, self.namespace.rules):
                self.toCsv(butler, root, path, dataId, cursor)
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        self.expFile.flush()
        self.mdFile.flush()
        self.polyFile.flush()
        self.polyFile.close()

    def toCsv(self, butler, root, path, dataId, cursor):
        """Extract/compute metadata for a single frame exposure, and
        store it in CSV files.
        """
        filename = os.path.join(root, path)
        if os.stat(filename).st_size < minExposureSize[self.camera]:
            msg = '{} : too small, possibly corrupt'.format(dataId) 
            if not self.namespace.strict:
                print >>sys.stderr, '*** Skipping ' + msg
                return
            else:
                raise RuntimeError(msg)
        scienceCcdExposureId = butler.get('ccdExposureId', dataId=dataId)
        # Check whether exposure has already been loaded
        if cursor:
            cursor.execute(str.format(
                'SELECT COUNT(*) FROM Science_Ccd_Exposure WHERE scienceCcdExposureId = {}',
                str(scienceCcdExposureId)))
            if cursor.fetchall()[0][0] == 1:
                msg = '{} : already loaded'.format(dataId)
                if not self.namespace.strict:
                    print >>sys.stderr, '*** Skipping ' + msg
                    return
                else:
                    raise RuntimeError(msg)
        # try to get PSF
        havePsf = False
        try:
            #get access to the psf as fast as possible: can load one pixel in 1/4 time to load full calexp
            miniBbox = afwGeom.Box2I(afwGeom.Point2I(0,0), afwGeom.Extent2I(1,1))
            exp = butler.get('calexp_sub', bbox=miniBbox, dataId=dataId, imageOrgin="LOCAL")
            psf = exp.getPsf()
            havePsf = psf != None
        except:
            pass
        if not havePsf:
            msg = '{} : PSF missing or corrupt'.format(dataId)
            if not self.namespace.strict:
                print >>sys.stderr, '*** Skipping ' + msg
                return
            else:
                raise RuntimeError(msg)
        # read image metadata and extract WCS/geometry metadata
        md = afwImage.readMetadata(filename)
        x0 = -md.get('LTV1') if md.exists('LTV1') else 0
        y0 = -md.get('LTV2') if md.exists('LTV2') else 0
        width = md.get('NAXIS1')
        height = md.get('NAXIS2')
        wcs = afwImage.makeWcs(md.deepCopy())
        cen = wcs.pixelToSky(x0 + 0.5*width - 0.5, y0 + 0.5*height - 0.5).toIcrs()
        corner1 = wcs.pixelToSky(x0 - 0.5, y0 - 0.5).toIcrs()
        corner2 = wcs.pixelToSky(x0 - 0.5, y0 + height - 0.5).toIcrs()
        corner3 = wcs.pixelToSky(x0 + width - 0.5, y0 + height - 0.5).toIcrs()
        corner4 = wcs.pixelToSky(x0 + width - 0.5, y0 - 0.5).toIcrs()
        # compute FWHM
        attr = measAlg.PsfAttributes(psf, x0 + width // 2, y0 + height // 2)
        fwhm = attr.computeGaussianWidth() * wcs.pixelScale().asArcseconds() * sigmaToFwhm
        # Build array of column values for one Science_Ccd_Exposure metadata row
        record = [scienceCcdExposureId]
        if self.camera in ('lsstsim', 'cfht'):
            filterName = md.get('FILTER').strip()
            filterId = afwImage.Filter(filterName, False).getId()
            obsStart = dafBase.DateTime(
                md.get('MJD-OBS'), dafBase.DateTime.MJD, dafBase.DateTime.UTC)
            record.append(dataId['visit'])
            if self.camera == 'lsstsim':
                record.extend([dataId['raftId'], dataId['raft'], dataId['sensorNum'], dataId['sensor'],])
            else:
                # CFHT camera doesn't have rafts
                record.extend([dataId['ccd'], dataId['ccdName'],])
            record.extend([filterId, filterName])
        elif self.camera == 'sdss':
            filterId = afwImage.Filter(dataId['filter'], False).getId()
            # compute start-of-exposure time from middle-of-exposure time and exposure duration
            expTime = md.get('EXPTIME') # s
            halfExpTimeNs = long(round(expTime * 500000000.0)) # 0.5 * expTime in ns
            obsStart = dafBase.DateTime(
                dafBase.DateTime(md.get('TIME-MID')).nsecs(dafBase.DateTime.TAI) - halfExpTimeNs,
                dafBase.DateTime.TAI)
            record.extend([
                dataId['run'],
                dataId['camcol'],
                filterId,
                dataId['field'],
                dataId['filter'],
            ])
        # WCS/geometry columns are the same across cameras
        record.extend([
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
            md.get('TIME-MID'),
            md.get('EXPTIME'),
            1, 1, 1,
        ])
        if self.camera in ('lsstsim', 'cfht'):
            # SDSS calexps do not go through CCD assembly/ISR, so these keys aren't available
            record.extend([md.get('RDNOISE'), md.get('SATURATE'), md.get('GAINEFF'),])
        # Append zero point, FWHM, and relative FITS file path
        record.extend([md.get('FLUXMAG0'), md.get('FLUXMAG0ERR'), fwhm, path])
        # Write out CSV record for exposure
        self.expFile.write(*record)
        # Write out CSV records for each FITS header key, value pair
        for name in md.paramNames():
            if md.typeOf(name) == md.TYPE_Int:
                self.mdFile.write(scienceCcdExposureId, name, 1, md.getInt(name), None, None)
            elif md.typeOf(name) == md.TYPE_Double:
                self.mdFile.write(scienceCcdExposureId, name, 1, None, md.getDouble(name), None)
            else:
                self.mdFile.write(scienceCcdExposureId, name, 1, None, None, str(md.get(name)))
        # Write out 4 corner TSV record.
        self.polyFile.write('\t'.join([
                str(scienceCcdExposureId),
                repr(corner1.getRa().asDegrees()), repr(corner1.getDec().asDegrees()),
                repr(corner2.getRa().asDegrees()), repr(corner2.getDec().asDegrees()),
                repr(corner3.getRa().asDegrees()), repr(corner3.getDec().asDegrees()),
                repr(corner4.getRa().asDegrees()), repr(corner4.getDec().asDegrees())]))
        self.polyFile.write('\n')
        print 'Processed {}'.format(dataId)


def dbLoad(ns, sql):
    """Load CSV files produced by CsvGenerator into database tables.
    """
    camera = ns.camera
    # Generate HTM IDs for 4-corner polygons
    subprocess.call([scisqlIndex, '-l', '10',
                     os.path.join(ns.outroot, 'Science_Ccd_Exposure_To_Htm10.tsv'),
                     os.path.join(ns.outroot, 'Science_Ccd_Exposure_Poly.tsv')])
    # Build Science_Ccd_Exposure LOAD statement
    loadStmt = str.format("""
        LOAD DATA LOCAL INFILE '{}'
        INTO TABLE Science_Ccd_Exposure
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
            scienceCcdExposureId,
            """,
        os.path.abspath(os.path.join(ns.outroot, 'Science_Ccd_Exposure.csv')))
    # ... ID columns are camera specific
    if camera == 'lsstsim':
        loadStmt += 'visit, raft, raftName, ccd, ccdName, filterId, filterName,'
    elif camera == 'cfht':
        loadStmt += 'visit, ccd, ccdName, filterId, filterName,'
    elif camera == 'sdss':
        loadStmt += 'run, camcol, filterId, field, filterName,'
    loadStmt += """
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
            nCombine, binX, binY,"""
    # ... and SDSS doesn't go through CCD assembly/ISR
    if camera in ('lsstsim', 'cfht'):
        loadStmt += """
            readNoise, saturationLimit, gainEff,"""
    loadStmt += """
            fluxMag0, fluxMag0Sigma, fwhm, path
        ) SET htmId20 = scisql_s2HtmId(ra, decl, 20),
              poly = scisql_s2CPolyToBin(corner1Ra, corner1Decl,
                                         corner2Ra, corner2Decl,
                                         corner3Ra, corner3Decl,
                                         corner4Ra, corner4Decl);
        SHOW WARNINGS;"""
    # Load Science_Ccd_Exposure table
    sql.execStmt(loadStmt)
    # Load Science_Ccd_Exposure_Metadata table with key,value pairs from FITS headers
    sql.execStmt(str.format("""
        LOAD DATA LOCAL INFILE '{}'
        INTO TABLE Science_Ccd_Exposure_Metadata
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        IGNORE 1 LINES (
            scienceCcdExposureId,
            metadataKey,
            exposureType,
            intValue,
            doubleValue,
            stringValue);
        SHOW WARNINGS;
        """, os.path.abspath(os.path.join(ns.outroot, 'Science_Ccd_Exposure_Metadata.csv'))))
    # Load HTM indexes for 4-corner polygons
    sql.execStmt(str.format("""
        LOAD DATA LOCAL INFILE '{}'
        INTO TABLE Science_Ccd_Exposure_To_Htm10 (
            scienceCcdExposureId,
            htmId10);
        SHOW WARNINGS;
        """, os.path.abspath(os.path.join(ns.outroot, 'Science_Ccd_Exposure_To_Htm10.tsv'))))


_validKeys = {
    'lsstsim': set(['visit', 'raft', 'sensor', 'ccd', 'channel', 'amp']),
    'sdss': set(['run', 'camcol', 'field', 'filter']),
    'cfht': set(['visit', 'ccd', 'amp']),
}


def main():
    parser = makeArgumentParser(description=
        'Converts processed single frame exposure metadata to CSV files '
        'suitable for loading into MySQL. If a database name is given, '
        'the CSVs are also loaded into that database. Make sure to run '
        'prepareDb.py with the appropriate --camera argument before '
        'database loads - this instantiates the camera specific LSST '
        'schema in the target database.')
    parser.add_argument("--camera", dest="camera", default="lsstSim",
        help="Name of desired camera (defaults to %(default)s)")
    ns = parser.parse_args()
    ns.camera = ns.camera.lower()
    if ns.camera not in _validKeys:
        parser.error('Unknown camera: {}. Choices (not case sensitive): {}'.format(
            ns.camera, _validKeys.keys()))
    ns.rules = makeRules(ns.id, ns.camera, _validKeys[ns.camera])
    sql = None
    doLoad = ns.database != None
    dirs = set(os.path.realpath(d) for d in ns.inroot)
    if len(dirs) != len(ns.inroot):
        parser.error('Input roots are not distinct (check for symlinks '
                     'to the same physical directory!)')
    if doLoad :
        if ns.user == None:
            parser.error('No database user name specified and $USER '
                         'is undefined or empty')
        sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    c = CsvGenerator(ns, not doLoad)
    c.csvAll(sql)
    if doLoad:
        dbLoad(ns, sql)


if __name__ == '__main__':
    main()

