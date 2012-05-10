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

import lsst.afw.table as afwTable
import lsst.ap.cluster as apCluster
import lsst.ap.utils as apUtils
from lsst.pipe.tasks.sourceAssoc import SourceAssocConfig

from lsst.datarel.ingest import makeArgumentParser, visitSkyTiles, pruneSkyTileDirs
from lsst.datarel.schema import *
from lsst.datarel.mysqlExecutor import MysqlExecutor


Task = collections.namedtuple('Task', ['kind', 'fitsPath', 'csvPath', 'config'])

_catalogClass = {
    'source': afwTable.SourceCatalog,
    'badSource': afwTable.SourceCatalog,
    'object': apCluster.SourceClusterCatalog,
}

def convert(task):
    try: 
        catalog = _catalogClass[task.kind].readFits(task.fitsPath)
        apUtils.writeCsv(catalog.cast(afwTable.BaseCatalog),
                         task.config.makeControl(),
                         makeMysqlCsvConfig().makeControl(),
                         task.csvPath,
                         True)
    except:
        return traceback.format_exc()
    return None

def convertAll(namespace, dbMappingConfig, sql=None):
    kinds = ('badSource', 'source', 'object')
    tasks = []
    stDirIdPairs = []
    saConfig = None
    saConfigFile = None
    srcSchema = None
    objSchema = None

    # Generate FITS to CSV conversion tasks, and write out sky-tile manifest
    for root, skyTileDir, skyTileId in visitSkyTiles(namespace, sql):
        outputSkyTile = False
        configFile = os.path.normpath(os.path.join(
            root, "..", "sourceAssoc_config", skyTileDir, "config.py"))
        config = SourceAssocConfig()
        config.load(configFile)
        if saConfig == None:
            saConfig = config
            saConfigFile = configFile
            dbMappingConfig.sourceConversion.nullableIntegers = [
                "parent", saConfig.sourceProcessing.clusterPrefix + ".id",]
        else:
            if saConfig != config:
                raise RuntimeError(str.format(
                    "sourceAssoc config files {} and {} do not match!",
                    saConfigFile, configFile))
        stOutDir = os.path.join(namespace.outroot, skyTileDir)
        for kind in kinds:
            fitsFile = os.path.join(root, skyTileDir, kind + '.fits')
            csvFile = os.path.join(stOutDir, kind + '.csv')
            if not os.path.isfile(fitsFile):
                continue
            if kind == 'object':
                if objSchema is None:
                    # read in FITS file and obtain schema
                    objSchema = _catalogClass[kind].readFits(fitsFile).getSchema()
                tasks.append(Task(kind, fitsFile, csvFile, dbMappingConfig.objectConversion))
            else:
                if srcSchema is None:
                    # read in FITS file and obtain schema
                    srcSchema = _catalogClass[kind].readFits(fitsFile).getSchema()
                tasks.append(Task(kind, fitsFile, csvFile, dbMappingConfig.sourceConversion))
            if not outputSkyTile:
                print str.format("Scheduling sky-tile {} for FITS->CSV conversion",
                                 skyTileId)
                outputSkyTile = True
                stDirIdPairs.append((stOutDir, skyTileId))
                try:
                    os.mkdir(stOutDir, 0755)
                except OSError, ex:
                    if ex.errno != errno.EEXIST:
                        raise

    # Spawn a worker process pool for the conversions and process them all.
    pool = multiprocessing.Pool(processes=namespace.jobs)
    results = pool.map(convert, tasks)
    ok = True
    for i, exc in enumerate(results):
        if exc != None:
            ok = False
            print >>sys.stderr, str.format(
                "*** Failed to convert {} to {}", tasks[i][1], tasks[i][2])
            print >>sys.stderr, exc
    if not ok:
         print >>sys.stderr, "\nFITS to CSV conversion failed!\n"
         sys.exit(1)
    return saConfig, srcSchema, objSchema, stDirIdPairs


def _exec(cursor, stmt):
    print >>sys.stderr, stmt
    cursor.execute(stmt)
    print >>sys.stderr, "\n"


def load(namespace, sql, srcStmts, objStmts, stDirIdPairs, asView):
    srcCreate, srcLoad, srcCanonical = srcStmts
    objCreate, objLoad, objCanonical = objStmts
    with closing(sql.getConn()) as conn:
        with closing(conn.cursor()) as cursor:
            if srcCreate != None:
                _exec(cursor, srcCreate)
                _exec(cursor, "ALTER TABLE RunSource DISABLE KEYS")
            if objCreate != None:
                _exec(cursor, objCreate)
                _exec(cursor, "ALTER TABLE RunObject DISABLE KEYS")
            for stOutDir, skyTileId in stDirIdPairs:
                print "-- Processing {}\n".format(stOutDir)
                try:
                    cursor.execute("INSERT INTO SkyTile (skyTileId) VALUES (%s)",
                                   (skyTileId,))
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
            if srcCanonical != None:
                if asView:
                    _exec(cursor, "DROP TABLE IF EXISTS Source")
                _exec(cursor, srcCanonical)
            if objCanonical != None:
                if asView:
                    _exec(cursor, "DROP TABLE IF EXISTS Object")
                _exec(cursor, objCanonical)


def main():
    # Setup command line options
    parser = makeArgumentParser(description="Program which converts "
        "source/object .fits files into CSV files and loads them "
        "into the database.", inRootsRequired=True, addRegistryOption=False)
    parser.add_argument(
        "-j", "--jobs", type=int, dest="jobs", default=4,
        help="Number of parallel job processes to launch when "
             "converting from FITS to CSV format")
    parser.add_argument(
        "--materialize", action="store_true", dest="materialize",
        help="Materialize canoncial Source/Object table views.")
    # TODO!! add finalize option, don't generate view/insert statements unless specified
    ns = parser.parse_args()
    sql = None
    if ns.database != None:
        if ns.user == None:
            parser.error("*** No database user name specified and $USER " +
                         "is undefined or empty")
        sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    else:
        parser.error("*** No database specified")
    # Perform FITS to CSV conversion
    dbmConfig = DbMappingConfig()
    dbmConfig.asView = not ns.materialize
    saConfig, srcSchema, objSchema, stOutDirs = convertAll(ns, dbmConfig, sql)
    if len(stOutDirs) == 0:
        print >>sys.stderr, "Nothing to do"
        return
    srcStmts = None, None, None
    if srcSchema is not None:
        srcStmts = sourceTableSql(srcSchema, dbmConfig, saConfig)
    objStmts = None, None, None
    if objSchema is not None:
        objStmts = objectTableSql(objSchema, dbmConfig, saConfig, "ugrizy")
    load(ns, sql, srcStmts, objStmts, stOutDirs, dbmConfig.asView)

if __name__ == "__main__":
    main()

