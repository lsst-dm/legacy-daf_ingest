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
import subprocess
import sys
from textwrap import dedent

from lsst.datarel.ingest import makeArgumentParser, visitSkyTiles, pruneSkyTileDirs
from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions


if not 'AP_DIR' in os.environ:
    print >>sys.stderr, "Please setup the ap package and try again"
    sys.exit(1)

AP_DIR = os.environ['AP_DIR']
refPosMatch = os.path.join(AP_DIR, 'bin', 'qa', 'refPosMatch.py')
refCcdFilter = os.path.join(AP_DIR, 'bin', 'qa', 'refCcdFilter.py')

Task = collections.namedtuple('Task', ['kind', 'boostPath', 'csvPath'])

def convert(task):
    if task.kind == 'object':
        pyfile = os.path.join(os.environ['AP_DIR'], 'bin', 'boostPt1Object2CSV.py')
        return subprocess.call(['python', pyfile, task.boostPath, task.csvPath])
    else:
        pyfile = os.path.join(os.environ['AP_DIR'], 'bin', 'boostPt1Source2CSV.py')
        return subprocess.call(['python', pyfile, task.boostPath, task.csvPath])

def convertAll(namespace, sql=None):
    kinds = ('badSource', 'source', 'object')
    tasks = []
    stDirIdPairs = []

    # Generate boost to CSV conversion tasks, and write out sky-tile manifest
    for root, skyTileDir, skyTileId in visitSkyTiles(namespace, sql):
        outputSkyTile = False
        stOutDir = os.path.join(namespace.outroot, skyTileDir)
        for kind in kinds:
            boostFile = os.path.join(root, skyTileDir, kind + '.boost')
            csvFile = os.path.join(stOutDir, kind + '.csv')
            if not os.path.isfile(boostFile):
                continue
            tasks.append(Task(kind, boostFile, csvFile))
            if not outputSkyTile:
                print str.format("Scheduling sky-tile {} for boost->CSV conversion",
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
    for i, r in enumerate(results):
        if r != 0:
            ok = False
            print >>sys.stderr, str.format(
                "*** Failed to convert {} to {}", tasks[i][1], tasks[i][2]) 
    if not ok:
         print >>sys.stderr, "Boost to CSV conversion failed!"
         sys.exit(1)
    return stDirIdPairs
 
def srcObjLoad(namespace, sql, stDirIdPairs):
    if stDirIdPairs == None:
        stDirIdPairs = pruneSkyTileDirs(
            namespace, glob.glob(os.path.join(namespace.outroot, "st[0-9]*"))) 
    with closing(sql.getConn()) as conn:
        with closing(conn.cursor()) as cursor:
            for stOutDir, skyTileId in stDirIdPairs:
                print "Processing " + stOutDir
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
                    print "  - loading " + csv
                    cursor.execute(dedent("""\
                        LOAD DATA LOCAL INFILE '%s' INTO TABLE Source 
                        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                            sourceId, scienceCcdExposureId, filterId,
                            objectId, movingObjectId, procHistoryId,
                            ra, raSigmaForDetection, raSigmaForWcs,
                            decl, declSigmaForDetection, declSigmaForWcs,
                            xFlux, xFluxSigma, yFlux, yFluxSigma,
                            raFlux, raFluxSigma, declFlux, declFluxSigma,
                            xPeak, yPeak,
                            raPeak, declPeak,
                            xAstrom, xAstromSigma, yAstrom, yAstromSigma,
                            raAstrom, raAstromSigma, declAstrom, declAstromSigma,
                            raObject, declObject,
                            taiMidPoint, taiRange,
                            psfFlux, psfFluxSigma,
                            apFlux, apFluxSigma,
                            flux_ESG, flux_ESG_Sigma,
                            petroFlux, petroFluxSigma,
                            flux_Gaussian, flux_Gaussian_Sigma,
                            nonGrayCorrFlux, nonGrayCorrFluxSigma,
                            atmCorrFlux, atmCorrFluxSigma,
                            apDia,
                            Ixx, IxxSigma, Iyy, IyySigma, Ixy, IxySigma,
                            psfIxx, psfIxxSigma, psfIyy, psfIyySigma, psfIxy, psfIxySigma,
                            e1_SG, e1_SG_Sigma, e2_SG, e2_SG_Sigma, 
                            resolution_SG,
                            shear1_SG, shear1_SG_Sigma, shear2_SG, shear2_SG_Sigma,
                            sourceWidth_SG, sourceWidth_SG_Sigma,
                            shapeFlag_SG,
                            snr, chi2,
                            sky, skySigma,
                            flagForAssociation, flagForDetection, flagForWcs
                        ) SET htmId20 = scisql_s2HtmId(ra, decl, 20);
                        """ % csv))
                csv = os.path.abspath(os.path.join(stOutDir, "object.csv"))
                if not os.path.isfile(csv):
                    continue
                print "  - loading " + csv
                cursor.execute(dedent("""\
                    LOAD DATA LOCAL INFILE '%s' INTO TABLE Object
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                        objectId, iauId,
                        ra_PS, ra_PS_Sigma, decl_PS, decl_PS_Sigma, radecl_PS_Cov,
                        ra_SG, ra_SG_Sigma, decl_SG, decl_SG_Sigma, radecl_SG_Cov,
                        raRange, declRange,
                        muRa_PS, muRa_PS_Sigma,
                        muDecl_PS, muDecl_PS_Sigma,
                        muRaDecl_PS_Cov,
                        parallax_PS, parallax_PS_Sigma,
                        canonicalFilterId,
                        extendedness, varProb,
                        earliestObsTime, latestObsTime, meanObsTime,
                        flags,
                        uNumObs, uExtendedness, uVarProb,
                        uRaOffset_PS, uRaOffset_PS_Sigma,
                        uDeclOffset_PS, uDeclOffset_PS_Sigma,
                        uRaDeclOffset_PS_Cov,
                        uRaOffset_SG, uRaOffset_SG_Sigma,
                        uDeclOffset_SG, uDeclOffset_SG_Sigma,
                        uRaDeclOffset_SG_Cov,
                        uLnL_PS, uLnL_SG,
                        uFlux_PS, uFlux_PS_Sigma,
                        uFlux_ESG, uFlux_ESG_Sigma,
                        uFlux_Gaussian, uFlux_Gaussian_Sigma,
                        uTimescale, uEarliestObsTime, uLatestObsTime,
                        uSersicN_SG, uSersicN_SG_Sigma,
                        uE1_SG, uE1_SG_Sigma, uE2_SG, uE2_SG_Sigma,
                        uRadius_SG, uRadius_SG_Sigma,
                        uFlux_PS_Num, uFlux_ESG_Num, uFlux_Gaussian_Num, uEllipticity_Num, uFlags,
                        gNumObs, gExtendedness, gVarProb,
                        gRaOffset_PS, gRaOffset_PS_Sigma,
                        gDeclOffset_PS, gDeclOffset_PS_Sigma,
                        gRaDeclOffset_PS_Cov,
                        gRaOffset_SG, gRaOffset_SG_Sigma,
                        gDeclOffset_SG, gDeclOffset_SG_Sigma,
                        gRaDeclOffset_SG_Cov,
                        gLnL_PS, gLnL_SG,
                        gFlux_PS, gFlux_PS_Sigma,
                        gFlux_ESG, gFlux_ESG_Sigma,
                        gFlux_Gaussian, gFlux_Gaussian_Sigma,
                        gTimescale, gEarliestObsTime, gLatestObsTime,
                        gSersicN_SG, gSersicN_SG_Sigma,
                        gE1_SG, gE1_SG_Sigma, gE2_SG, gE2_SG_Sigma,
                        gRadius_SG, gRadius_SG_Sigma,
                        gFlux_PS_Num, gFlux_ESG_Num, gFlux_Gaussian_Num, gEllipticity_Num, gFlags,
                        rNumObs, rExtendedness, rVarProb,
                        rRaOffset_PS, rRaOffset_PS_Sigma,
                        rDeclOffset_PS, rDeclOffset_PS_Sigma,
                        rRaDeclOffset_PS_Cov,
                        rRaOffset_SG, rRaOffset_SG_Sigma,
                        rDeclOffset_SG, rDeclOffset_SG_Sigma,
                        rRaDeclOffset_SG_Cov,
                        rLnL_PS, rLnL_SG,
                        rFlux_PS, rFlux_PS_Sigma,
                        rFlux_ESG, rFlux_ESG_Sigma,
                        rFlux_Gaussian, rFlux_Gaussian_Sigma,
                        rTimescale, rEarliestObsTime, rLatestObsTime,
                        rSersicN_SG, rSersicN_SG_Sigma,
                        rE1_SG, rE1_SG_Sigma, rE2_SG, rE2_SG_Sigma,
                        rRadius_SG, rRadius_SG_Sigma,
                        rFlux_PS_Num, rFlux_ESG_Num, rFlux_Gaussian_Num, rEllipticity_Num, rFlags,
                        iNumObs, iExtendedness, iVarProb,
                        iRaOffset_PS, iRaOffset_PS_Sigma,
                        iDeclOffset_PS, iDeclOffset_PS_Sigma,
                        iRaDeclOffset_PS_Cov,
                        iRaOffset_SG, iRaOffset_SG_Sigma,
                        iDeclOffset_SG, iDeclOffset_SG_Sigma,
                        iRaDeclOffset_SG_Cov,
                        iLnL_PS, iLnL_SG,
                        iFlux_PS, iFlux_PS_Sigma,
                        iFlux_ESG, iFlux_ESG_Sigma,
                        iFlux_Gaussian, iFlux_Gaussian_Sigma,
                        iTimescale, iEarliestObsTime, iLatestObsTime,
                        iSersicN_SG, iSersicN_SG_Sigma,
                        iE1_SG, iE1_SG_Sigma, iE2_SG, iE2_SG_Sigma,
                        iRadius_SG, iRadius_SG_Sigma,
                        iFlux_PS_Num, iFlux_ESG_Num, iFlux_Gaussian_Num, iEllipticity_Num, iFlags,
                        zNumObs, zExtendedness, zVarProb,
                        zRaOffset_PS, zRaOffset_PS_Sigma,
                        zDeclOffset_PS, zDeclOffset_PS_Sigma,
                        zRaDeclOffset_PS_Cov,
                        zRaOffset_SG, zRaOffset_SG_Sigma,
                        zDeclOffset_SG, zDeclOffset_SG_Sigma,
                        zRaDeclOffset_SG_Cov,
                        zLnL_PS, zLnL_SG,
                        zFlux_PS, zFlux_PS_Sigma,
                        zFlux_ESG, zFlux_ESG_Sigma,
                        zFlux_Gaussian, zFlux_Gaussian_Sigma,
                        zTimescale, zEarliestObsTime, zLatestObsTime,
                        zSersicN_SG, zSersicN_SG_Sigma,
                        zE1_SG, zE1_SG_Sigma, zE2_SG, zE2_SG_Sigma,
                        zRadius_SG, zRadius_SG_Sigma,
                        zFlux_PS_Num, zFlux_ESG_Num, zFlux_Gaussian_Num, zEllipticity_Num, zFlags,
                        yNumObs, yExtendedness, yVarProb,
                        yRaOffset_PS, yRaOffset_PS_Sigma,
                        yDeclOffset_PS, yDeclOffset_PS_Sigma,
                        yRaDeclOffset_PS_Cov,
                        yRaOffset_SG, yRaOffset_SG_Sigma,
                        yDeclOffset_SG, yDeclOffset_SG_Sigma,
                        yRaDeclOffset_SG_Cov,
                        yLnL_PS, yLnL_SG,
                        yFlux_PS, yFlux_PS_Sigma,
                        yFlux_ESG, yFlux_ESG_Sigma,
                        yFlux_Gaussian, yFlux_Gaussian_Sigma,
                        yTimescale, yEarliestObsTime, yLatestObsTime,
                        ySersicN_SG, ySersicN_SG_Sigma,
                        yE1_SG, yE1_SG_Sigma, yE2_SG, yE2_SG_Sigma,
                        yRadius_SG, yRadius_SG_Sigma,
                        yFlux_PS_Num, yFlux_ESG_Num, yFlux_Gaussian_Num, yEllipticity_Num, yFlags
                    ) SET htmId20 = scisql_s2HtmId(ra_PS, decl_PS, 20);
                    """ % csv))

def referenceMatch(namespace, sql):
    objectTsv = os.path.abspath(os.path.join(namespace.outroot, 'objDump.tsv'))
    sourceTsv = os.path.abspath(os.path.join(namespace.outroot, 'srcDump.tsv'))
    expMetaDumpPaf = os.path.abspath(os.path.join(namespace.outroot, 'expMetaDump.paf'))
    dumpPaf = os.path.abspath(os.path.join(namespace.outroot, 'dump.paf'))
    filtCsv = os.path.abspath(os.path.join(namespace.outroot, 'refFilt.csv'))
    objectMatchCsv = os.path.abspath(os.path.join(namespace.outroot, 'refObjMatch.csv'))
    sourceMatchCsv = os.path.abspath(os.path.join(namespace.outroot, 'refSrcMatch.csv'))
    # Filter reference catalog
    refCsvCols = ','.join(['refObjectId',
                           'isStar',
                           'varClass',
                           'ra',
                           'decl',
                           'htmId20',
                           'gLat',
                           'gLon',
                           'sedName,'
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
                          ])
    filtCsvCols = ','.join([refCsvCols,
                            'uCov',
                            'gCov',
                            'rCov',
                            'iCov',
                            'zCov',
                            'yCov',
                           ])
    filtArgs = ['python', refCcdFilter, namespace.refCatalog, filtCsv, '-F', refCsvCols]
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
                            ORDER BY scienceCcdExposureId""", f, ['-B', '-N'])
        with open(expMetaDumpPaf, "wb") as f:
            f.write("""csvDialect: {
                           delimiter: "\t"
                       }
                    """)
        filtArgs.append("-E")
        filtArgs.append(expMetaDumpPaf)
    for f in expMeta:
        filtArgs.append("-e")
        filtArgs.append(f)
    subprocess.call(filtArgs)
    # Dump object table
    with open(objectTsv, "wb") as f:
        sql.execStmt("""SET myisam_sort_buffer_size=1000000000;
                        SELECT objectId, ra_PS, decl_PS, meanObsTime
                        FROM Object
                        ORDER BY decl_PS""", f, ['-B', '-N'])
    # Dump source table
    with open(sourceTsv, "wb") as f:
        sql.execStmt("""SET myisam_sort_buffer_size=1000000000;
                        SELECT sourceId, ra, decl, taiMidPoint
                        FROM Source
                        ORDER BY decl""", f, ['-B', '-N'])
    # Write out match policy for source/object dumps
    with open(dumpPaf, "wb") as f:
        f.write("""fieldNames: "objectId" "ra" "decl" "epoch"
                   csvDialect: {
                       delimiter: "\t"
                   }
                """)
    # Match reference objects to objects
    subprocess.call(['python', refPosMatch, filtCsv, objectTsv, objectMatchCsv,
                     '-s', '-r', str(namespace.radius), '-P', dumpPaf, '-F', filtCsvCols])
    # Match reference objects to sources
    subprocess.call(['python', refPosMatch, filtCsv, sourceTsv, sourceMatchCsv,
                     '-r', str(namespace.radius), '-P', dumpPaf, '-F', filtCsvCols])
    if namespace.noMatchLoad:
        return
    # Load filtered reference catalog and matches
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' INTO TABLE SimRefObject
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (%s);
                 """ % (filtCsv, filtCsvCols))
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' INTO TABLE RefObjMatch
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                        refObjectId, objectId,
                        refRa, refDec, angSep,
                        nRefMatches, nObjMatches,
                        closestToRef, closestToObj,
                        flags);
                """ % objectMatchCsv)
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' INTO TABLE RefSrcMatch
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                        refObjectId, sourceId,
                        refRa, refDec, angSep,
                        nRefMatches, nSrcMatches,
                        closestToRef, closestToSrc,
                        flags);
                """ % sourceMatchCsv)

def main():
    # Setup command line options
    parser = makeArgumentParser(description="Program which performs some combination of "
        "converting source/object .boost files into CSV files, loading those CSV files, "
        "reference matching, and loading reference match results.",
        inRootsRequired=False, addRegistryOption=False)
    parser.add_argument(
        "-j", "--jobs", type=int, dest="jobs", default=4,
        help="Number of parallel job processes to launch when "
             "converting from boost to CSV format")
    parser.add_argument(
        "--no-conversion", action="store_true", dest="noConversion",
        help="Don't convert from boost to CSV format")
    parser.add_argument(
        "--no-src-obj-load", action="store_true", dest="noSrcObjLoad",
        help="Don't load sources and objects")
    parser.add_argument(
        "--no-match-load", action="store_true", dest="noMatchLoad",
        help="Don't load reference match results")
    parser.add_argument(
        "-m", "--match", action="store_true", dest="match",
        help="Perform reference catalog to source/object matching. "
             "Currently only works for LSST Sim runs.")
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
             "metadata must have previously been loaded into the database given "
             "by --database.")
    ns = parser.parse_args()
    sql = None
    if ns.database != None:
        if ns.user == None:
            parser.error("*** No database user name specified and $USER " +
                         "is undefined or empty")
        sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    if sql == None:
         if ns.match:
             parser.error("Reference matching requested, but no database "
                          "was specified (-d)")
         if not ns.noSrcObjLoad:
             print >>sys.stderr, "*** No database specified : sources and objects will not be loaded"
             ns.noSrcObjLoad = True
    # Perform boost to CSV conversion
    stOutDirs = None
    if not ns.noConversion and len(ns.inroot) > 0:
        stOutDirs = convertAll(ns, sql)
    if not ns.noSrcObjLoad:
        srcObjLoad(ns, sql, stOutDirs)
    if ns.match:
        referenceMatch(ns, sql)

if __name__ == "__main__":
    main()

