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

import argparse
import collections
from contextlib import closing
import errno
import glob
import multiprocessing
import os, os.path
import sys
import traceback

import lsst.daf.persistence as dafPersistence
import lsst.afw.table as afwTable
import lsst.ap.cluster as apCluster
import lsst.ap.utils as apUtils
from lsst.ap.tasks.sourceAssoc import SourceAssocConfig

from lsst.datarel.ingest import makeArgumentParser, makeRules
from lsst.datarel.schema import makeMysqlCsvConfig, sourceTableSql, objectTableSql, DbMappingConfig
from lsst.datarel.mysqlExecutor import MysqlExecutor
from lsst.datarel.datasetScanner import getMapperClass, DatasetScanner


Task = collections.namedtuple(
    'Task', ['dataset', 'protoPath', 'fitsPath', 'csvPath', 'config'])

_catalogClass = {
    'source': afwTable.SourceCatalog,
    'badSource': afwTable.SourceCatalog,
    'object': apCluster.SourceClusterCatalog,
}

_filters = {
    'lsstsim': ['u', 'g', 'r', 'i', 'z', 'y',],
    'sdss': ['u', 'g', 'r', 'i', 'z',],
    'cfht': ['u','g','r','i','z', 'i2',],
}

def convert(task):
    """Run a CSV conversion Task."""
    try:
        prototype = _catalogClass[task.dataset].readFits(task.protoPath)
        catalog = _catalogClass[task.dataset].readFits(task.fitsPath)
        if (catalog.getSchema() != prototype.getSchema()):
            return str.format('Schema for {} does not match prototype ({})!',
                              task.fitsPath, task.protoPath)
        apUtils.writeCsv(catalog.cast(afwTable.BaseCatalog),
                         task.config.makeControl(),
                         makeMysqlCsvConfig().makeControl(),
                         task.csvPath,
                         True,
                         False)
    except:
        return traceback.format_exc()
    return None

def sourcePrototypePath(namespace):
    """Return the name of the empty FITS table file (stored in the output
    directory) containing the prototypical Source Schema; all C++ SourceCatalog
    instances must match this Schema for CSV conversion to succeed.
    """
    return os.path.join(namespace.outroot, 'sourcePrototype.fits')

def objectPrototypePath(namespace):
    """Return the name of the empty FITS table file (stored in the output
    directory) containing the prototypical Object Schema; all C++
    SourceClusterCatalog instances must match this Schema for CSV
    conversion to succeed.
    """
    return os.path.join(namespace.outroot, 'objectPrototype.fits')

def sourceAssocConfigPath(namespace):
    """Return the path to the canonical SourceAssoc config file (stored
    in the output directory). 
    """
    return os.path.join(namespace.outroot, 'sourceAssoc_config.py')

def getPrototypicalSchemas(namespace):
    """Return a tuple (srcSchema, objSchema) corresponding to the
    prototypical SourceCatalog and SourceClusterCatalog Schemas. These
    are read in from empty FITS tables in the output directory if such
    tables exist. If an empty FITS table for the prototypical
    SourceCatalog/SourceClusterCatalog does not exist in the output
    directory, None is returned for srcSchema/objSchema.
    """
    srcSchema, objSchema = None, None
    p = sourcePrototypePath(namespace)
    if os.path.isfile(p):
        srcSchema = afwTable.SourceCatalog.readFits(p).getSchema()
    p = objectPrototypePath(namespace)
    if os.path.isfile(p):
        objSchema = apCluster.SourceClusterCatalog.readFits(p).getSchema()
    return srcSchema, objSchema

def getSourceAssocConfig(namespace):
    """Return the canonical SourceAssocConfig, obtained by loading the output
    directory config file. If the output directory has yet to be populated with
    this file, None is returned.
    """
    saConfig = None
    p = sourceAssocConfigPath(namespace)
    if os.path.isfile(p):
        saConfig = SourceAssocConfig()
        saConfig.load(p)
    return saConfig

def mungeDbMappingConfig(dbMappingConfig, saConfig):
    """Make sure that the Source field named <clusterPrefix>.id
    is treated as a NULLable INTEGER/BIGINT when ingesting into the
    database.
    """
    dbMappingConfig.sourceConversion.nullableIntegers = [
        "parent", saConfig.sourceProcessing.clusterPrefix + ".id",]

class Context(object):
    """Tracks CSV conversion tasks, sky-tiles, canonical SourceAssocConfig,
    and prototypical Schemas for SourceCatalogs/SourceClusterCatalogs.
    """
    def __init__(self, namespace):
        self.tasks = []       # list of conversion tasks
        self.skyTiles = set() # set of sky-tiles for which conversions were performed
        # canonical SourceAssocConfig and config file path
        self.saConfig = getSourceAssocConfig(namespace)
        # prototypical SourceCatalog/SourceClusterCatalog Schemas
        self.srcSchema, self.objSchema = getPrototypicalSchemas(namespace)
        # paths to empty FITS tables containing prototypical
        # SourceCatalog/SourceClusterCatalog Schemas
        self.srcProto = sourcePrototypePath(namespace)
        self.objProto = objectPrototypePath(namespace)


def csvAll(namespace, dbMappingConfig, sql=None):
    """Scans the list of input roots (namespace.inroot) for 'source',
    'badSource', and 'object' datasets. For each dataset found:

    - check that the corresponding sky-tile has not already been ingested
    - create a CSV conversion Task for the corresponding FITS file

    Along the way, the output directory is populated with a canonical
    SourceAssocConfig, and empty FITS tables that contain the prototypical
    SourceCatalog and SourceClusterCatalog schemas.

    Then, using namespace.jobs parallel processes, each of the FITS files
    is converted to CSV. This includes a check of the to-be-converted table
    Schema against the prototype.

    The return value of this function is the set of sky-tile IDs for which
    conversions were performed.
    """
    ctx = Context(namespace)
    # If possible, create a database connection and cursor - it will be
    # used to avoid CSV conversion for sky-tiles that have already been
    # loaded.
    conn = sql.getConn() if sql else None
    cursor = conn.cursor() if conn else None

    for root in namespace.inroot:
        print 'Ingesting from ' + root
        if hasattr(namespace, 'registry'):
            registry = namespace.registry
        else:
            registry = os.path.join(root, 'registry.sqlite3')
        # Create a butler for the input root
        cls = getMapperClass(namespace.camera)
        cameraMapper = cls(root=root, registry=registry)
        butler = dafPersistence.ButlerFactory(mapper=cameraMapper).create()
        for dataset in ('source', 'badSource', 'object'):
            # Create a scanner for the dataset of interest
            scanner = DatasetScanner(dataset=dataset,
                                     camera=namespace.camera,
                                     cameraMapper=cameraMapper)
            for path, dataId in scanner.walk(root, namespace.rules):
                if cursor:
                    # check whether the sky-tile has already been loaded
                    cursor.execute('SELECT COUNT(*) FROM SkyTile WHERE skyTileId = {}'.format(
                        dataId['skyTile']))
                    if cursor.fetchall()[0][0] == 1:
                        msg = '{} : already loaded {} dataset'.format(dataId, dataset)
                        if not namespace.strict:
                            print >>sys.stderr, '*** Skipping ' + msg
                            continue
                        else:
                            raise RuntimeError(msg)
                csvOne(ctx, butler, namespace, dbMappingConfig, dataset, root, path, dataId)
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    # Spawn a worker process pool for the conversion Tasks and process them all.
    pool = multiprocessing.Pool(processes=namespace.jobs)
    results = pool.map(convert, ctx.tasks)
    ok = True
    # Check that all CSV conversion tasks succeeded
    for i, exc in enumerate(results):
        if exc != None:
            ok = False
            print >>sys.stderr, str.format(
                "*** Failed to convert {} to {}", tasks[i][1], tasks[i][2])
            print >>sys.stderr, exc
    if not ok:
         # At least one failure - bail out.
         print >>sys.stderr, "\nFITS to CSV conversion failed!\n"
         sys.exit(1)
    return ctx.skyTiles


def csvOne(ctx, butler, namespace, dbMappingConfig, dataset, root, path, dataId):
    """Append CSV conversion Task for a single FITS file to ctx.tasks,
    and update ctx.skyTiles. If there is no canonical SourceAssocConfig
    for the output directory yet, make the sourceAssoc_config for the
    current sky-tile canonical. Otherwise, check that it matches the
    canonical one. Similarly, if there is no prototypical SourceCatalog/
    SourceClusterCatalog Schema yet, make the one from this FITS file
    the prototype. Schema consistency checks are deferred to the CSV
    conversion Tasks.
    """
    config = butler.get("sourceAssoc_config", dataId=dataId, immediate=True)
    if ctx.saConfig is None:
        # Have no canonical SourceAssocConfig for the output directory,
        # so make the one we just read the canonical one.
        ctx.saConfig = config
        ctx.saConfig.save(sourceAssocConfigPath(namespace))
    else:
        if ctx.saConfig != config:
            raise RuntimeError(str.format(
                "{} : sourceAssoc_config does not match config in {}",
                dataId, sourceAssocConfigPath(namespace)))
    fitsFile = os.path.join(root, path)
    stOutDir = os.path.join(namespace.outroot, 'st' + str(dataId['skyTile']))
    csvFile = os.path.join(stOutDir, dataset + '.csv')

    if dataId['skyTile'] not in ctx.skyTiles:
        print "Scheduling sky-tile {} for FITS->CSV conversion".format(dataId['skyTile'])
        ctx.skyTiles.add(dataId['skyTile'])
        # make sure sky-tile output directory exists
        try:
            os.mkdir(stOutDir, 0755)
        except OSError, ex:
            if ex.errno != errno.EEXIST:
                raise
    if dataset == 'object':
        if ctx.objSchema is None:
            # Have no prototypical SourceClusterCatalog schema yet - make
            # the one from this FITS table the prototypical one.
            cat = apCluster.SourceClusterCatalog.readFits(fitsFile)
            cat = apCluster.SourceClusterCatalog(cat.getTable())
            cat.writeFits(ctx.objProto)
            ctx.objSchema = cat.getSchema()
        ctx.tasks.append(Task(dataset, ctx.objProto, fitsFile, csvFile,
                              dbMappingConfig.objectConversion))
    else:
        if ctx.srcSchema is None:
            # Have no prototypical SourceCatalog schema yet - make
            # the one from this FITS table the prototypical one.
            cat = afwTable.SourceCatalog.readFits(fitsFile)
            cat = afwTable.SourceCatalog(cat.getTable())
            cat.writeFits(ctx.srcProto)
            ctx.srcSchema = cat.getSchema()
        ctx.tasks.append(Task(dataset, ctx.srcProto, fitsFile, csvFile,
                              dbMappingConfig.sourceConversion))


def _exec(cursor, stmt):
    """Print statement to execute, then execute it."""
    print >>sys.stderr, stmt
    cursor.execute(stmt)
    print >>sys.stderr, "\n"

def _getSkyTiles(namespace):
    """Extract sky-tile ID set from output directory names."""
    skyTiles = set()
    for d in glob.glob(os.path.join(namespace.outroot, "st[0-9]*")):
        if not os.path.isdir(d):
            continue
        try:
            skyTile = int(os.path.basename(d)[2:])
            skyTiles.add(skyTile)
        except:
            pass
    return skyTiles


def load(namespace, sql, srcStmts, objStmts, skyTiles):
    if skyTiles == None:
        skyTiles = _getSkyTiles(namespace)
    srcCreate, srcLoad, _ = srcStmts
    objCreate, objLoad, _ = objStmts
    with closing(sql.getConn()) as conn:
        with closing(conn.cursor()) as cursor:
            if srcCreate != None:
                _exec(cursor, srcCreate)
                _exec(cursor, "ALTER TABLE RunSource DISABLE KEYS;")
            if objCreate != None:
                _exec(cursor, objCreate)
                _exec(cursor, "ALTER TABLE RunObject DISABLE KEYS;")
            for skyTile in skyTiles:
                stOutDir = os.path.join(namespace.outroot, "st" + str(skyTile))
                print "-- Processing {}\n".format(stOutDir)
                try:
                    cursor.execute("INSERT INTO SkyTile (skyTileId) VALUES (%s);",
                                   (skyTile,))
                except Exception, e:
                    if hasattr(e, "__getitem__") and e[0] == 1062:
                        # Integrity error, duplicate for PRIMARY KEY
                        msg = str.format("sky-tile {} : already ingested",
                                         skyTileId)
                        if namespace.strict:
                            raise RuntimeError(msg)
                        else:
                            print >>sys.stderr, "*** Skipping " + msg
                            continue
                # Load sources
                for fileName in ("badSource.csv", "source.csv"):
                    csv = os.path.abspath(os.path.join(stOutDir, fileName))
                    if not os.path.isfile(csv):
                        continue
                    _exec(cursor, srcLoad.format(fileName=csv))
                csv = os.path.abspath(os.path.join(stOutDir, "object.csv"))
                if not os.path.isfile(csv):
                    continue
                _exec(cursor, objLoad.format(fileName=csv))


def main():
    # Setup command line options
    parser = makeArgumentParser(description="Program which converts "
        "source/object .fits files into CSV files and loads them "
        "into the database.", inRootsRequired=False, addRegistryOption=False)
    parser.add_argument("--camera", dest="camera", default="lsstSim",
        help="Name of desired camera (defaults to %(default)s)")
    parser.add_argument(
        "-j", "--jobs", type=int, dest="jobs", default=4,
        help="Number of parallel job processes to launch when "
             "converting from FITS to CSV format")
    parser.add_argument(
        "--no-convert", action="store_true", dest="noConvert",
        help="Don't convert from FITS to CSV format")
    parser.add_argument(
        "--no-load", action="store_true", dest="noLoad",
        help="Don't load sources and objects")
    parser.add_argument(
        "--create-views", action="store_true", dest="createViews",
        help="Create views corresponding to the canonical Source/Object after loading.")
    parser.add_argument(
        "--insert", action="store_true", dest="insert",
        help="Insert into canonical Source/Object tables after loading.")
    ns = parser.parse_args()
    ns.camera = ns.camera.lower()
    if ns.camera not in _filters:
        parser.error('Unknown camera: {}. Choices (not case sensitive): {}'.format(
            ns.camera, _filters.keys()))
    ns.rules = makeRules(ns.id, ns.camera, ['skyTile',])
    sql = None
    if ns.database != None:
        if ns.user == None:
            parser.error("*** No database user name specified and $USER " +
                         "is undefined or empty")
        sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    elif not ns.noLoad or ns.createViews or ns.insert:
        parser.error("*** No database specified")
    if ns.noLoad and ns.noConvert and not ns.createViews and not ns.insert:
        print >>sys.stderr, "Nothing to do"
        return

    dbmConfig = DbMappingConfig()
    dbmConfig.asView = ns.createViews
    skyTiles = None
    if not ns.noConvert:
        if not ns.inroot:
            parser.error("At least one input root must be specified "
                         "when FITS->CSV conversion is turned on")
        # Perform FITS to CSV conversion
        skyTiles = csvAll(ns, dbmConfig, sql)
    saConfig = getSourceAssocConfig(ns)
    srcSchema, objSchema = getPrototypicalSchemas(ns)
    if saConfig is None:
        parser.error(str.format(
            "Missing sourceAssoc config file {}", sourceAssocConfigPath(ns)))
    if srcSchema is None and objSchema is None:
        parser.error(str.format(
            "Missing both source and object prototypes ({} and {})",
            sourcePrototypePath(ns), objectPrototypePath(ns)))
    srcStmts = None, None, None
    if srcSchema is not None:
        srcStmts = sourceTableSql(srcSchema, dbmConfig, saConfig)
    objStmts = None, None, None
    if objSchema is not None:
        objStmts = objectTableSql(objSchema, dbmConfig, saConfig,
                                  _filters[ns.camera])
    if not ns.noLoad:
        load(ns, sql, srcStmts, objStmts, skyTiles)
    if ns.createViews or ns.insert:
        if srcStmts[2] != None:
            if ns.createViews:
                sql.execStmt("DROP TABLE IF EXISTS Source;")
            sql.execStmt(srcStmts[2])
        if objStmts[2] != None:
            if ns.createViews:
                sql.execStmt("DROP TABLE IF EXISTS Object;")
            sql.execStmt(objStmts[2])

if __name__ == "__main__":
    main()

