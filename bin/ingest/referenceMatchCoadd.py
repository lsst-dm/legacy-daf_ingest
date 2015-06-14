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

from lsst.daf.ingest.ingest import makeArgumentParser
from lsst.daf.ingest.mysqlExecutor import MysqlExecutor, addDbOptions
from referenceMatch import refColumns
from ingestCoadd import coaddNames

if not 'AP_DIR' in os.environ:
    print >>sys.stderr, "Please setup the ap package and try again"
    sys.exit(1)

AP_DIR = os.environ['AP_DIR']
refPosMatch = os.path.join(AP_DIR, 'bin', 'qa', 'refPosMatch.py')
refFilter = os.path.join(AP_DIR, 'bin', 'qa', 'refFilter.py')

def referenceMatch(namespace, sql):
    camera = namespace.camera
    config = apMatch.ReferenceMatchConfig()
    config.expIdKey = 'coaddId'
    config.ref.idColumn = 'refObjectId'
    config.ref.fieldNames = list(refColumns[camera])
    config.pos.outputFields = []
    config.posDialect.delimiter = '\t'
    config.radius = namespace.radius
    apUtils.makeMapper(namespace.camera) # defines filters

    # If the reference catalog has not been pre-filtered, filter it
    if namespace.alreadyFiltered:
        filtCsv = namespace.refCatalog
        config.ref.fieldNames.extend([f + "ExposureCount" for f in apUtils.getFilterNames()])
    else:
        refFilterConfigFile = os.path.abspath(os.path.join(namespace.outroot, 'refFilterConfig.py'))
        filtCsv = os.path.abspath(os.path.join(namespace.outroot, 'refFilt.csv'))
        config.validate()
        config.save(refFilterConfigFile)
        filtArgs = ['python', refFilter,
                    '--config-file=' + refFilterConfigFile,
                    '--camera=' + namespace.camera,
                    filtCsv,
                    namespace.refCatalog,
                   ]
        for em in namespace.expMeta:
            filtArgs.extend(em)
        subprocess.check_call(filtArgs)
        config.ref.fieldNames.extend([f + "ExposureCount" for f in apUtils.getFilterNames()])
        if not namespace.noLoad:
            # Load filtered reference catalog and matches
            sql.execStmt(str.format(
                """LOAD DATA LOCAL INFILE '{}' INTO TABLE RefObject
                   FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ({});
                   SHOW WARNINGS;""",
                filtCsv, ','.join(config.ref.fieldNames)))

    config.ref.outputFields = []
    config.doOutputRefExtras = False
    config.validate()
    refMatchConfigFile = os.path.abspath(os.path.join(namespace.outroot, 'refMatchConfig.py'))
    config.save(refMatchConfigFile)

    for coaddName in namespace.coaddNames:
        sourceTsv = os.path.abspath(os.path.join(namespace.outroot, coaddName + 'SrcDump.tsv'))
        srcMatchCsv = os.path.abspath(os.path.join(namespace.outroot, coaddName + 'SrcMatch.csv'))
        CoaddName = coaddName[0].upper() + coaddName[1:]
        # Dump object table
        with open(sourceTsv, 'wb') as f:
            sql.execStmt(str.format(
                """SET myisam_sort_buffer_size=1000000000;
                   SELECT {}SourceId AS id, ra, decl FROM {}Source ORDER BY decl""",
                coaddName, CoaddName), f, ['-B'])

        # Note - if Source contains no rows, MySQL will output an empty file,
        # even if you've asked it to write out column names in a header line!
        if os.path.getsize(sourceTsv) == 0:
            with open(sourceTsv, 'wb') as f:
                f.write('"id"\t"ra"\t"decl"\n')
        subprocess.check_call(['python', refPosMatch,
                               '--config-file=' + refMatchConfigFile,
                               srcMatchCsv,
                               filtCsv,
                               sourceTsv,
                              ])
        if namespace.noLoad:
            continue
        sql.execStmt(str.format(
            """LOAD DATA LOCAL INFILE '{}' INTO TABLE Ref{}SrcMatch
               FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                   refObjectId, {}SourceId,
                   angSep, nRefMatches, nSrcMatches,
                   closestToRef, closestToSrc);
               SHOW WARNINGS;
            """, srcMatchCsv, CoaddName, coaddName))

def main():
    # Setup command line options
    parser = argparse.ArgumentParser(description="Perform matching of "
        "coadd Source tables against a reference catalog, and optionally load "
        "the results.")
    addDbOptions(parser)
    parser.add_argument("--coadd-names", nargs="*", dest="coaddNames",
        help="Names of coadd types to ingest data for. Omitting this "
             "option will reference match coadd-sources for all coadd "
             "types defined for the camera.")
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
        "--already-filtered", dest="alreadyFiltered", action="store_true",
        help="Indicates that the reference catalog specified via -R has "
             "already been filtered against exposures; this allows the "
             "RefObject table from e.g. SFM reference-matching to be re-used.")
    parser.add_argument(
        "-r", "--radius", type=float, dest="radius", default=1.0,
        help="Reference object to source cluster match radius, arcsec. "
             "The default is %(default)g arcsec.")
    parser.add_argument(
        "-e", "--exposure-metadata", nargs="+", action="append", dest="expMeta",
        help="The names of one or more exposure metadata key-value CSV files. "
             "This option may be specified more than once. Ignored if "
             "--already-filtered is specfied. ")
    parser.add_argument(
       "outroot", help="Output directory for CSV files")
    ns = parser.parse_args()
    sql = None
    if ns.database == None:
        parser.error("No database specified (--database)")
    if ns.user == None:
        parser.error("*** No database user name specified and $USER " +
                     "is undefined or empty")
    ns.camera = ns.camera.lower()
    if ns.camera not in refColumns:
        parser.error("Unknown camera: {}. Choices (not case sensitive): {}".format(
            ns.camera, refColumns.keys()))
    if not ns.alreadyFiltered and ns.expMeta == None:
        parser.error("Reference catalog filtering requested, but "
                     "--exposure-metadata was not specified.")
    if ns.coaddNames:
        for n in ns.coaddNames:
            if n not in coaddNames[ns.camera]:
                parser.error(str.format(
                    'Coadd type {} is not defined for camera {}. Valid coadd types are {}',
                    n, ns.camera, coaddNames[ns.camera]))
    else:
        ns.coaddNames = list(coaddNames[ns.camera])
    sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    referenceMatch(ns, sql)

if __name__ == "__main__":
    main()

