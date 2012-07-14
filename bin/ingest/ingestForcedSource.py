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
from lsst.datarel.schema import makeMysqlCsvConfig, forcedSourceTableSql
from lsst.datarel.datasetScanner import getMapperClass, DatasetScanner

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

def coaddForcedSourceTable(coaddName):
    """Return forced source table name, given a coadd name."""
    return coaddName[0].upper() + coaddName[1:] + 'ForcedSource'

def _getDataset(butler, dataset, dataId, strict, warn):
    try:
        ds = butler.get(dataset, dataId=dataId, immediate=True)
    except:
        ds = None
    if ds == None:
        msg = '{} : Failed to retrieve {} dataset'.format(dataId, dataset)
        if strict:
            raise RuntimeError(msg)
        elif warn:
            print >>sys.stderr, '*** Skipping ' + msg
    return ds

class ForcedSourceProcessingConfig(apCluster.SourceProcessingConfig):
    def setDefaults(self):
        self.multiBand = False
        self.coadd = False
        self.exposurePrefix = "exposure"
        self.clusterPrefix = None

class CsvGenerator(object):
    def __init__(self, namespace, compress=True):
        self.namespace = namespace
        self.camera = namespace.camera
        self.sourceInfo = None
        self.csvConfig = makeMysqlCsvConfig()
        cfg = apUtils.CsvConversionConfig()
        cfg.nullableIntegers = ['parent', 'cluster.id']
        self.csvConversionConfig = cfg
        self.sourceProcessingConfig = ForcedSourceProcessingConfig()

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
            print '\tScanning FS for forcedsources dataset'
            scanner = DatasetScanner(dataset='forcedsources',
                                     camera=self.camera,
                                     cameraMapper=cameraMapper)
            # scan the root for matching coadd calexps
            for path, dataId in scanner.walk(root, self.namespace.rules):
                self.toCsv(butler, root, path, dataId, cursor)
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    def toCsv(self, butler, root, path, dataId, cursor):
        """Convert forced sources to CSV files for database ingest.
        """
        filename = os.path.join(root, path)
        # Retrieve forced sources and forced photometry config
        sources = _getDataset(butler, 'forcedsources', dataId,
                              strict=self.namespace.strict, warn=True)
        if sources == None:
            return
        sfmConfig = _getDataset(butler,'forcedPhot_config',
                                dataId, strict=self.namespace.strict, warn=True)
        if sfmConfig == None:
            return
        md = _getDataset(butler, 'calexp_md', dataId,
                strict=self.namespace.strict, warn=True)
        exposureId = _getDataset(butler, 'ccdExposureId', dataId,
                strict=self.namespace.strict, warn=True)

        if self.namespace.fixIds:
            bits = _getDataset(butler, 'ccdExposureId_bits', dataId,
                    strict=self.namespace.strict, warn=True)
            if bits > 48:
                raise RuntimeError("Insufficient bits for forcedSourceId "
                        "(need 16, have {})".format(64 - bits))
            idKey = sources.schema.find("id").key
            for s in sources:
                s.set(idKey, exposureId * 65536L + s.get(idKey))

        # Build ExposureInfo from FITS header
        try:
            expInfo = apMatch.ExposureInfo(md, exposureId)
        except:
            msg = str.format('{} : failed to construct ExposureInfo object from calexp FITS header', dataId)
            if self.namespace.strict:
                print >>sys.stderr, '*** Skipping ' + msg
                return
            else:
                raise RuntimeError(msg)
        # Get appropriate  SourceProcessingConfig
        spc = self.sourceProcessingConfig
        appendSources = True
        if self.sourceInfo == None:
            appendSources = False
            # Construct output schema and schema mapper
            outputSourceTable, schemaMapper = apCluster.makeOutputSourceTable(
                sources.getTable(), spc.makeControl())
            self.sourceInfo = (
                outputSourceTable, schemaMapper, sfmConfig.measurement.slots, sfmConfig.measurement.prefix)
        outputSourceTable, schemaMapper, measSlots, measPrefix = self.sourceInfo
        # Verify consistency of slot mappings and measurement field name prefix
        if measSlots != sfmConfig.measurement.slots or measPrefix != sfmConfig.measurement.prefix:
            msg = str.format('{} : Inconsitent measurement slot mapping of prefix for {} coadd',
                             dataId, coaddName)
            if self.namespace.strict:
                print >>sys.stderr, '*** Skipping ' + msg
                return
            else:
                raise RuntimeError(msg)
        # Denormalize forcedSources - computes sky-coordinate errors, and 
        # attach exposure ID, filter, etc...
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
        # Write out forcedSources that are not invalid (i.e. have an ra,dec) as CSV records
        apUtils.writeCsv(
            outputSources.cast(afwTable.BaseCatalog),
            self.csvConversionConfig.makeControl(),
            self.csvConfig.makeControl(),
            os.path.join(self.namespace.outroot,
                coaddForcedSourceTable(self.namespace.coaddName) + '.csv'),
            False,
            appendSources)
        print 'Processed {}'.format(dataId)


def dbLoad(ns, sql, csvGenerator):
    """Load CSV files produced by CsvGenerator into database tables.
    """
    camera = ns.camera
    sourceTable = coaddForcedSourceTable(ns.coaddName)
    fileName = os.path.join(ns.outroot, sourceTable + '.csv')
    # Generate SQL for the coadd-source table
    outputSourceTable, _, measSlots, measPrefix = csvGenerator.sourceInfo
    sourceStmts = forcedSourceTableSql(
            ns.coaddName,
            outputSourceTable.getSchema(),
            csvGenerator.csvConversionConfig,
            ns.createViews,
            csvGenerator.sourceProcessingConfig,
            measSlots,
            measPrefix)
    # Create run-specific forced source table and disable indexes
    sql.execStmt(sourceStmts[0])
    sql.execStmt('ALTER TABLE Run{} DISABLE KEYS;'.format(sourceTable))
    # Load run-specific forced source table
    sql.execStmt(sourceStmts[1].format(fileName=fileName) + '\nSHOW WARNINGS;')
    # Create view or insert into canonical forced source table 
    if ns.createViews:
        sql.execStmt('DROP TABLE IF EXISTS {};'.format(sourceTable))
    sql.execStmt(sourceStmts[2] + '\nSHOW WARNINGS;')


_validKeys = {
    'lsstsim': set(['visit', 'raft', 'sensor',]),
    'sdss': set(['run', 'camcol', 'field', 'filter',]),
    'cfht': set([]),
}


def main():
    parser = makeArgumentParser(description=
        'Converts forced photometry source tables to CSV files '
        'suitable for loading into MySQL. If a database name is given, '
        'the CSVs are also loaded into that database. Make sure to run '
        'prepareDb.py with the appropriate --camera argument before '
        'database loads - this instantiates the camera specific LSST '
        'schema in the target database.')
    parser.add_argument("--camera", dest="camera", default="lsstSim",
        help="Name of desired camera (defaults to %(default)s)")
    parser.add_argument("--coadd-name", dest="coaddName",
        help="Name of coadd type to ingest data for.")
    parser.add_argument("--create-views", action="store_true",
            dest="createViews",
            help="Create views corresponding to the canonical ForcedSource after loading.")
    parser.add_argument("--fix-ids", action="store_true", dest="fixIds",
            help="Fix ids by including exposureId")

    ns = parser.parse_args()
    ns.camera = ns.camera.lower()
    if ns.camera not in _validKeys:
        parser.error('Unknown camera: {}. Choices (not case sensitive): {}'.format(
            ns.camera, _validKeys.keys()))

    if ns.camera == "sdss":
        # For config
        try:
            import lsst.obs.sdss.forcedPhot
        except:
            pass
    elif ns.camera == "lsstSim":
        # For config
        try:
            import lsst.obs.lsstSim.forcedPhot
        except:
            pass

    if ns.coaddName not in coaddNames[ns.camera]:
        parser.error(str.format(
            'Coadd type {} is not defined for camera {}. Valid coadd types are {}',
            ns.coaddName, ns.camera, coaddNames[ns.camera]))
    sql = None
    doLoad = ns.database != None

    if doLoad and ns.user == None:
        parser.error('No database user name specified and $USER '
                'is undefined or empty')

    ns.rules = makeRules(ns.id, ns.camera, _validKeys[ns.camera])
    dirs = set(os.path.realpath(d) for d in ns.inroot)
    if len(dirs) != len(ns.inroot):
        parser.error('Input roots are not distinct (check for symlinks '
                     'to the same physical directory!)')
    csvGenerator = CsvGenerator(ns, not doLoad)
    csvGenerator.csvAll(sql)

    if doLoad:
        sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
        dbLoad(ns, sql, csvGenerator)


if __name__ == '__main__':
    main()

