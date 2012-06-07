#! /usr/bin/env python

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

import argparse
import os, os.path
import subprocess
import sys

import lsst.ap.match as apMatch
import lsst.ap.utils as apUtils

from lsst.datarel.ingest import makeArgumentParser
from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions

if not 'AP_DIR' in os.environ:
    print >>sys.stderr, "Please setup the ap package and try again"
    sys.exit(1)

AP_DIR = os.environ['AP_DIR']
refPosMatch = os.path.join(AP_DIR, 'bin', 'qa', 'refPosMatch.py')
refCcdFilter = os.path.join(AP_DIR, 'bin', 'qa', 'refCcdFilter.py')


_refColumns = {
    'lsstsim': ['refObjectId',
                'isStar',
                'varClass',
                'ra',
                'decl',
                'htmId20',
                'gLat',
                'gLon',
                'sedName',
                'uMag',
                'gMag',
                'rMag',
                'iMag',
                'zMag',
                'yMag',
                'muRa',
                'muDecl',
                'parallax',
                'vRad',
                'redshift',
                'semiMajorBulge',
                'semiMinorBulge',
                'semiMajorDisk',
                'semiMinorDisk',
               ],
    'sdss':    ['refObjectId',
                'flags',
                'run',
                'rerun',
                'camcol',
                'field',
                'obj',
                'mode',
                'type',
                'isStar',
                'ra',
                'decl',
                'htmId20',
                'uMag',
                'gMag',
                'rMag',
                'iMag',
                'zMag',
                'uMagSigma',
                'gMagSigma',
                'rMagSigma',
                'iMagSigma',
                'zMagSigma',
               ],
}

def referenceMatch(namespace, sql):
    camera = namespace.camera.lower()
    objectTsv = os.path.abspath(os.path.join(namespace.outroot, 'objDump.tsv'))
    sourceTsv = os.path.abspath(os.path.join(namespace.outroot, 'srcDump.tsv'))
    refCcdFilterConfigFile = os.path.abspath(os.path.join(namespace.outroot, 'refCcdFilterConfig.py'))
    refSrcMatchConfigFile = os.path.abspath(os.path.join(namespace.outroot, 'refSrcMatchConfig.py'))
    refObjMatchConfigFile = os.path.abspath(os.path.join(namespace.outroot, 'refObjMatchConfig.py'))
    filtCsv = os.path.abspath(os.path.join(namespace.outroot, 'refFilt.csv'))
    objectMatchCsv = os.path.abspath(os.path.join(namespace.outroot, 'refObjMatch.csv'))
    sourceMatchCsv = os.path.abspath(os.path.join(namespace.outroot, 'refSrcMatch.csv'))

    config = apMatch.ReferenceMatchConfig()

    # Setup parameters for reference matching
    config.expIdKey = 'scienceCcdExposureId'
    config.ref.idColumn = 'refObjectId'
    config.ref.fieldNames = list(_refColumns[camera])
    config.pos.outputFields = []
    config.posDialect.delimiter = '\t'
    config.radius = namespace.radius

    expMeta = []
    if namespace.expMeta != None:
        for em in namespace.expMeta:
            expMeta.extend(em)
    if len(expMeta) == 0:
        # Dump metadata from database
        expMeta.append(os.path.join(namespace.outroot, "Science_Ccd_Exposure_Metadata.tsv"))
        with open(expMeta[0], "wb") as f:
            sql.execStmt("""SET myisam_sort_buffer_size=1000000000;
                            SELECT scienceCcdExposureId,
                                   metadataKey,
                                   exposureType,
                                   intValue,
                                   doubleValue,
                                   stringValue
                            FROM Science_Ccd_Exposure_Metadata
                            ORDER BY scienceCcdExposureId""", f, ['-B'])
        config.expDialect.delimiter = '\t'
    config.validate()
    config.save(refCcdFilterConfigFile)
    filtArgs = ['python', refCcdFilter,
                '--config-file=' + refCcdFilterConfigFile,
                '--camera=' + namespace.camera,
                filtCsv,
                namespace.refCatalog,
               ]
    filtArgs.extend(expMeta)
    subprocess.check_call(filtArgs)
    # Dump object table
    with open(objectTsv, "wb") as f:
        sql.execStmt("""SET myisam_sort_buffer_size=1000000000;
                        SELECT objectId AS id, ra, decl, obsTimeMean AS epoch
                        FROM Object
                        ORDER BY decl""", f, ['-B'])
    # Dump source table
    with open(sourceTsv, "wb") as f:
        sql.execStmt("""SET myisam_sort_buffer_size=1000000000;
                        SELECT sourceId AS id, ra, decl, timeMid AS epoch
                        FROM Source
                        ORDER BY decl""", f, ['-B'])

    # Add filter coverage columns to reference field names
    apUtils.makeMapper(namespace.camera) # defines filters
    config.ref.fieldNames.extend([f + "ExposureCount" for f in apUtils.getFilterNames()])
    config.ref.outputFields = []

    # Match reference objects to sources
    config.validate()
    config.save(refSrcMatchConfigFile)
    # Note - if Source contains no rows, MySQL will output an empty file,
    # even if you've asked it to write out column names in a header line!
    if os.path.getsize(sourceTsv) == 0:
        with open(sourceTsv, 'wb') as f:
            f.write('"id"\t"ra"\t"decl"\n')
    subprocess.check_call(['python', refPosMatch,
                           '--config-file=' + refSrcMatchConfigFile,
                           sourceMatchCsv,
                           filtCsv,
                           sourceTsv,
                          ])

    # Match reference objects to objects
    config.parallaxThresh = float('inf') # turn off parallax corrections
    config.validate()
    config.save(refObjMatchConfigFile)
    if os.path.getsize(objectTsv) == 0:
        with open(objectTsv, 'wb') as f:
            f.write('"id"\t"ra"\t"decl"\n')
    subprocess.check_call(['python', refPosMatch,
                           '--config-file=' + refSrcMatchConfigFile,
                           objectMatchCsv,
                           filtCsv,
                           objectTsv,
                          ])
    if namespace.noLoad:
        return
    # Load filtered reference catalog and matches
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' INTO TABLE RefObject
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (%s);
                 """ % (filtCsv, ','.join(config.ref.fieldNames)))
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' INTO TABLE RefObjMatch
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                        refObjectId, objectId,
                        refRa, refDecl, angSep,
                        nRefMatches, nObjMatches,
                        closestToRef, closestToObj,
                        flags);
                """ % objectMatchCsv)
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' INTO TABLE RefSrcMatch
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                        refObjectId, sourceId,
                        refRa, refDecl, angSep,
                        nRefMatches, nSrcMatches,
                        closestToRef, closestToSrc,
                        flags);
                """ % sourceMatchCsv)

def main():
    # Setup command line options
    parser = argparse.ArgumentParser(description="Perform matching of "
        "Source/Object against a reference catalog, and optionally load "
        "the results.")
    addDbOptions(parser)
    parser.add_argument(
        "-d", "--database", dest="database", required=True,
        help="MySQL database to load CSV files into.")
    parser.add_argument(
        "--no-load", action="store_true", dest="noLoad",
        help="Don't load reference match results into the database")
    parser.add_argument("--camera", dest="camera", default="lsstSim",
        help="Name of desired camera (defaults to %(default)s)")
    parser.add_argument(
        "-R", "--ref-catalog", dest="refCatalog",
        default="/lsst/DC3/data/obs/ImSim/ref/simRefObject-2011-08-01-0.csv",
        help="Reference catalog CSV file (%(default)s).")
    parser.add_argument(
        "-r", "--radius", type=float, dest="radius", default=1.0,
        help="Reference object to source cluster match radius, arcsec. "
             "The default is %(default)g arcsec.")
    parser.add_argument(
        "-e", "--exposure-metadata", nargs="+", action="append", dest="expMeta",
        help="The names of one or more exposure metadata key-value CSV files. "
             "This option may be specified more than once. If omitted, exposure "
             "metadata must have previously been loaded into the "
             "Science_Ccd_Exposure_Metadata table in database given by "
             "--database.")
    parser.add_argument(
        "outroot", help="Output directory for CSV files")
    ns = parser.parse_args()
    sql = None
    if ns.database == None:
        parser.error("No database specified (--database)")
    if ns.user == None:
        parser.error("*** No database user name specified and $USER " +
                     "is undefined or empty")
    camera = ns.camera.lower()
    if camera not in _refColumns:
        parser.error("Unknown camera: {}. Choices (not case sensitive): {}".format(
            camera, _refColumns.keys()))

    sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    referenceMatch(ns, sql)

if __name__ == "__main__":
    main()

