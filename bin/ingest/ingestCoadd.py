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
import re
import subprocess
import sys

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersistence
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage
import lsst.afw.table as afwTable
import lsst.meas.algorithms as measAlg
import lsst.ap.match as apMatch
import lsst.ap.cluster as apCluster
import lsst.ap.utils as apUtils

# Hack to be able to read multiShapelet configs
try:
    import lsst.meas.extensions.multiShapelet
except:
    pass

from lsst.datarel.csvFileWriter import CsvFileWriter
from lsst.datarel.mysqlExecutor import MysqlExecutor
from lsst.datarel.ingest import makeArgumentParser, makeRules
from lsst.datarel.schema import makeMysqlCsvConfig, coaddSourceTableSql
from lsst.datarel.datasetScanner import getMapperClass, DatasetScanner
from lsst.datarel.utils import getDataset, getPsf

if not 'SCISQL_DIR' in os.environ:
    print >>sys.stderr, 'Please setup the scisql package and try again'
    sys.exit(1)

scisqlIndex = os.path.join(os.environ['SCISQL_DIR'], 'bin', 'scisql_index')
sigmaToFwhm = 2.0*math.sqrt(2.0*math.log(2.0))

# List of coadd dataset names for each camera
coaddNames = {
    'lsstsim': ['goodSeeing', 'deep', 'chiSquared',],
    'sdss': ['goodSeeing', 'deep', 'chiSquared', 'keith',],
    'cfht': ['goodSeeing', 'deep', 'chiSquared',],
}

# Exposure type code for a coadd
_exposureType = {
    'goodSeeing': 3,
    'deep': 4,
    'chiSquared': 5,
    'keith': 6,
}

def coaddExposureTable(coaddName):
    """Return exposure table name, given a coadd name.""" 
    return coaddName[0].upper() + coaddName[1:] + 'Coadd'

def coaddSourceTable(coaddName):
    """Return source table name, given a coadd name."""
    return coaddName[0].upper() + coaddName[1:] + 'Source'

class CsvGenerator(object):
    def __init__(self, namespace, compress=True):
        self.namespace = namespace
        self.camera = namespace.camera
        self.expFile = {}
        self.mdFile = {}
        self.polyFile = {}
        self.sourceInfo = {}
        self.sourceProcessingConfig = {}
        spc = apCluster.SourceProcessingConfig()
        chiSquaredSpc = apCluster.SourceProcessingConfig()
        chiSquaredSpc.exposurePrefix = spc.exposurePrefix = "coadd"
        chiSquaredSpc.clusterPrefix = spc.clusterPrefix = ""
        spc.multiBand = False
        chiSquaredSpc.multiBand = True
        chiSquaredSpc.coadd = spc.coadd = True
        chiSquaredSpc.badFlagFields = spc.badFlagFields = []
        self.csvConfig = makeMysqlCsvConfig()
        cfg = apUtils.CsvConversionConfig()
        cfg.nullableIntegers = ['parent']
        self.csvConversionConfig = cfg

        for coaddName in namespace.coaddNames:
            if coaddName == 'chiSquared':
                self.sourceProcessingConfig[coaddName] = chiSquaredSpc
            else:
                self.sourceProcessingConfig[coaddName] = spc
            expTable = coaddExposureTable(coaddName)
            self.expFile[coaddName] = CsvFileWriter(
                path=os.path.join(namespace.outroot, expTable + '.csv'),
                compress=compress)
            self.mdFile[coaddName] = CsvFileWriter(
                path=os.path.join(namespace.outroot, expTable + '_Metadata.csv'),
                compress=compress)
            # Writer column name header line for calexp metadata CSV - note this
            # is purely to pass field names on to the reference filtering code later.
            # That means we can get away with using a generic name for the coadd exposure
            # ID column (coaddId), rather than the database column name (e.g. goodSeeingCoddId)
            self.mdFile[coaddName].write(
                'coaddId', 'metadataKey', 'exposureType',
                'intValue', 'doubleValue', 'stringValue')
            self.polyFile[coaddName] = open(os.path.join(namespace.outroot, expTable + '_Poly.tsv'), 'wb')
            self.sourceInfo[coaddName] = None

    def csvAll(self, sql=None):
        """Extract/compute metadata for all coadds matching
        at least one data ID specification, and store it in CSV files.
        Also convert sources extracted from the coadd to CSV (after
        denormalization).
        """
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
            # Loop over types of coadds
            for coaddName in self.namespace.coaddNames:
                print '\tScanning FS for {}Coadd_calexp dataset'.format(coaddName)
                scanner = DatasetScanner(dataset=coaddName + 'Coadd_calexp',
                                         camera=self.camera,
                                         cameraMapper=cameraMapper)
                # scan the root for matching coadd calexps
                for path, dataId in scanner.walk(root, self.namespace.rules):
                    self.toCsv(coaddName, butler, root, path, dataId, cursor)
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        for coaddName in self.namespace.coaddNames:
            self.expFile[coaddName].flush()
            self.mdFile[coaddName].flush()
            self.polyFile[coaddName].flush()
            self.polyFile[coaddName].close()

    def toCsv(self, coaddName, butler, root, path, dataId, cursor):
        """Extract/compute metadata for a coadd exposure, and convert
        the associated sources to CSV files for database ingest.
        """
        filename = os.path.join(root, path)
        expTable = coaddExposureTable(coaddName)
        coaddId = butler.get(coaddName + 'CoaddId', dataId=dataId, immediate=True)
        # Check whether exposure has already been loaded
        if cursor:
            cursor.execute('SELECT COUNT(*) FROM {} WHERE {}CoaddId = {}'.format(
                expTable, coaddName, coaddId))
            if cursor.fetchall()[0][0] == 1:
                msg = '{} : already loaded {} coadd'.format(dataId, coaddName)
                if not self.namespace.strict:
                    print >>sys.stderr, '*** Skipping ' + msg
                    return
                else:
                    raise RuntimeError(msg)
        # try to get PSF - not - may not always be available, e.g. if PSF matching was turned off.
        # the PSF is in the coadd and we don't want want the coadd, but there is presently no way
        # to just get metadata, so load one pixel of the coadd
        initPsf = getPsf(butler, coaddName + "Coadd", dataId=dataId, strict=False, warn=False)
        psf = getPsf(butler, coaddName + 'Coadd_calexp', dataId=dataId, strict=False, warn=False)
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
        if initPsf != None:
            attr = measAlg.PsfAttributes(psf, x0 + width // 2, y0 + height // 2)
            matchedFwhm = attr.computeGaussianWidth() * wcs.pixelScale().asArcseconds() * sigmaToFwhm
        else:
            matchedFwhm = None
        if psf != None:
            attr = measAlg.PsfAttributes(psf, x0 + width // 2, y0 + height // 2)
            measuredFwhm = attr.computeGaussianWidth() * wcs.pixelScale().asArcseconds() * sigmaToFwhm
        else:
            measuredFwhm = None
        # Build array of column values for one coaddd exposure row
        record = [coaddId]
        if coaddName == 'keith':
           assert(self.camera == 'sdss')
           # Have SDSS run, camcol, field, filter, rerun ID components
           record.extend([dataId['run'], dataId['rerun'], dataId['camcol'], dataId['field']])
        else:
           # Have tract and patch ID components
           record.extend([dataId['tract'], dataId['patch']])
        if coaddName != 'chiSquared':
           # filter id and name
           record.append(afwImage.Filter(dataId['filter'], False).getId())
           record.append(dataId['filter'])
        # Remaining columns are the same across cameras/coadd types
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
            md.get('FLUXMAG0'), md.get('FLUXMAG0ERR'),
            matchedFwhm, measuredFwhm, path
        ])
        # Retrieve coadd sources and processCoadd config
        sources = getDataset(butler, coaddName + 'Coadd_src', dataId=dataId,
                             strict=self.namespace.strict, warn=True)
        if sources == None:
            return
        sfmConfig = getDataset(butler, coaddName + '_processCoadd_config', dataId=dataId,
                               strict=self.namespace.strict, warn=True)
        if sfmConfig == None:
            return

        if self.namespace.fixIds:
            bits = butler.get(coaddName + 'CoaddId_bits', dataId=dataId,
                    immediate=True)
            if bits > 48:
                raise RuntimeError("Insufficient bits for forcedSourceId "
                        "(need 16, have {})".format(64 - bits))
            idKey = sources.schema.find("id").key
            for s in sources:
                s.set(idKey, coaddId * 65536L + s.get(idKey))

        # Build ExposureInfo from FITS header
        try:
            expInfo = apMatch.ExposureInfo(md, coaddId)
        except:
            msg = str.format('{} : failed to construct {} ExposureInfo object from coadd FITS header',
                             dataId, coaddName)
            if self.namespace.strict:
                print >>sys.stderr, '*** Skipping ' + msg
                return
            else:
                raise RuntimeError(msg)
        # Get appropriate  SourceProcessingConfig
        spc = self.sourceProcessingConfig[coaddName]
        appendSources = True
        if self.sourceInfo[coaddName] == None:
            appendSources = False
            # Construct output schema and schema mapper
            outputSourceTable, schemaMapper = apCluster.makeOutputSourceTable(
                sources.getTable(), spc.makeControl())
            self.sourceInfo[coaddName] = (
                outputSourceTable, schemaMapper, sfmConfig.measurement.slots, sfmConfig.measurement.prefix)
        outputSourceTable, schemaMapper, measSlots, measPrefix = self.sourceInfo[coaddName]
        # Verify consistency of slot mappings and measurement field name prefix
        if measSlots != sfmConfig.measurement.slots or measPrefix != sfmConfig.measurement.prefix:
            msg = str.format('{} : Inconsistent measurement slot mapping of prefix for {} coadd',
                             dataId, coaddName)
            if self.namespace.strict:
                print >>sys.stderr, '*** Skipping ' + msg
                return
            else:
                raise RuntimeError(msg)
        # Denormalize coadd sources - computes sky-coordinate errors, and 
        # attach coadd ID, filter, etc...
        outputSourceTable = outputSourceTable.clone()
        outputSources = afwTable.SourceCatalog(outputSourceTable)
        badSources = afwTable.SourceCatalog(outputSourceTable)
        invalidSources = afwTable.SourceCatalog(outputSourceTable)
        apCluster.processSources(
            sources,
            expInfo,
            None,
            spc.makeControl(),
            schemaMapper,
            outputSources,
            badSources,
            invalidSources)
        assert(len(badSources) == 0) # spc.badFlagFields is empty!
        # Write out coadd sources that are not invalid (i.e. have an ra,dec) as CSV records
        apUtils.writeCsv(
            outputSources.cast(afwTable.BaseCatalog),
            self.csvConversionConfig.makeControl(),
            self.csvConfig.makeControl(),
            os.path.join(self.namespace.outroot, coaddSourceTable(coaddName) + '.csv'),
            False,
            appendSources)
        # Write out CSV record for exposure
        self.expFile[coaddName].write(*record)
        # Write out CSV records for each FITS header key
        expType = _exposureType[coaddName]
        for name in md.paramNames():
            if md.typeOf(name) == md.TYPE_Int:
                self.mdFile[coaddName].write(coaddId, name, expType, md.getInt(name), None, None)
            elif md.typeOf(name) == md.TYPE_Double:
                self.mdFile[coaddName].write(coaddId, name, expType, None, md.getDouble(name), None)
            else:
                self.mdFile[coaddName].write(coaddId, name, expType, None, None, str(md.get(name)))
        # Write out 4 corner TSV record; later turned into HTM indexes by scisql.
        self.polyFile[coaddName].write('\t'.join([
                str(coaddId),
                repr(corner1.getRa().asDegrees()), repr(corner1.getDec().asDegrees()),
                repr(corner2.getRa().asDegrees()), repr(corner2.getDec().asDegrees()),
                repr(corner3.getRa().asDegrees()), repr(corner3.getDec().asDegrees()),
                repr(corner4.getRa().asDegrees()), repr(corner4.getDec().asDegrees())]))
        self.polyFile[coaddName].write('\n')
        print 'Processed {}'.format(dataId)


def dbLoad(ns, sql, csvGenerator):
    """Load CSV files produced by CsvGenerator into database tables.
    """
    camera = ns.camera
    for coaddName in ns.coaddNames:
        expTable = coaddExposureTable(coaddName)
        sourceTable = coaddSourceTable(coaddName)
        fileName = os.path.join(ns.outroot, sourceTable + '.csv')
        # Generate SQL for the coadd-source table
        outputSourceTable, _, measSlots, measPrefix = csvGenerator.sourceInfo[coaddName]
        sourceStmts = coaddSourceTableSql(
            coaddName,
            outputSourceTable.getSchema(),
            csvGenerator.csvConversionConfig,
            ns.createViews,
            csvGenerator.sourceProcessingConfig[coaddName],
            measSlots,
            measPrefix)
        # Create run-specific coadd-source table and disable indexes
        sql.execStmt(sourceStmts[0])
        sql.execStmt('ALTER TABLE Run{} DISABLE KEYS;'.format(sourceTable))
        # Load run-specific coadd-source table
        sql.execStmt(sourceStmts[1].format(fileName=fileName) +
                     '\nSHOW WARNINGS;')
        # Create view or insert into canonical coadd-source table 
        if ns.createViews:
            sql.execStmt('DROP TABLE IF EXISTS {};'.format(sourceTable))
        sql.execStmt(sourceStmts[2])
        # Generate HTM IDs for 4-corner coadd polygons
        subprocess.call([scisqlIndex, '-l', '10',
                         os.path.join(ns.outroot, expTable + '_To_Htm10.tsv'),
                         os.path.join(ns.outroot, expTable + '_Poly.tsv')])
        # Build LOAD statement for Coadd exposure table
        loadStmt = str.format("""
            LOAD DATA LOCAL INFILE '{}'
            INTO TABLE {}
            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                {}CoaddId,
                """,
            os.path.abspath(os.path.join(ns.outroot, expTable + '.csv')),
            expTable,
            coaddName)
        if coaddName == 'keith':
            loadStmt += 'run, rerun, camcol, field,'
        else:
            loadStmt += 'tract, patch,'
        if coaddName != 'chiSquared':
            loadStmt += ' filterId, filterName,'
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
                fluxMag0, fluxMag0Sigma,
                matchedFwhm, measuredFwhm,
                path
            ) SET htmId20 = scisql_s2HtmId(ra, decl, 20),
                  poly = scisql_s2CPolyToBin(corner1Ra, corner1Decl,
                                             corner2Ra, corner2Decl,
                                             corner3Ra, corner3Decl,
                                             corner4Ra, corner4Decl);
            SHOW WARNINGS;"""
        # Load Coadd exposure table
        sql.execStmt(loadStmt)
        # Load key-value table with FITS header cards
        sql.execStmt(str.format("""
            LOAD DATA LOCAL INFILE '{}'
            INTO TABLE {}_Metadata
            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
            IGNORE 1 LINES (
                {}CoaddId,
                metadataKey,
                exposureType,
                intValue,
                doubleValue,
                stringValue);
            SHOW WARNINGS;""",
            os.path.abspath(os.path.join(ns.outroot, expTable + '_Metadata.csv')),
            expTable,
            coaddName))
        # Load HTM indexes for 4-corner polygons
        sql.execStmt(str.format("""
            LOAD DATA LOCAL INFILE '{}'
            INTO TABLE {}_To_Htm10 (
                {}CoaddId,
                htmId10);
            SHOW WARNINGS;""",
            os.path.abspath(os.path.join(ns.outroot, expTable + '_To_Htm10.tsv')),
            expTable,
            coaddName))


_validKeys = {
    'lsstsim': set(['tract', 'patch', 'filter',]),
    'sdss': set(['tract', 'patch', 'filter', 'run', 'camcol', 'field',]),
    'cfht': set(['tract', 'patch', 'filter',]),
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
    parser.add_argument("--coadd-names", nargs="*", dest="coaddNames",
        help="Names of coadd types to ingest data for. Omitting this "
             "option will result in ingest of all coadd types defined "
             "for the camera.")
    parser.add_argument(
        "--create-views", action="store_true", dest="createViews",
        help="Create views corresponding to the canonical Source/Object after loading.")
    parser.add_argument(
        "--fix-ids", action="store_true", dest="fixIds",
        help="Fix id field by including coadd id")


    ns = parser.parse_args()
    ns.camera = ns.camera.lower()
    if ns.camera not in _validKeys:
        parser.error('Unknown camera: {}. Choices (not case sensitive): {}'.format(
            ns.camera, _validKeys.keys()))
    if ns.coaddNames:
        for n in ns.coaddNames:
            if n not in coaddNames[ns.camera]:
                parser.error(str.format(
                    'Coadd type {} is not defined for camera {}. Valid coadd types are {}',
                    n, ns.camera, coaddNames[ns.camera]))
    else:
        ns.coaddNames = list(coaddNames[ns.camera])
    ns.rules = makeRules(ns.id, ns.camera, _validKeys[ns.camera])
    sql = None
    doLoad = ns.database != None
    dirs = set(os.path.realpath(d) for d in ns.inroot)
    if len(dirs) != len(ns.inroot):
        parser.error('Input roots are not distinct (check for symlinks '
                     'to the same physical directory!)')
    if doLoad:
        if ns.user == None:
            parser.error('No database user name specified and $USER '
                         'is undefined or empty')
        sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    csvGenerator = CsvGenerator(ns, not doLoad)
    csvGenerator.csvAll(sql)
    if doLoad:
        dbLoad(ns, sql, csvGenerator)


if __name__ == '__main__':
    main()

