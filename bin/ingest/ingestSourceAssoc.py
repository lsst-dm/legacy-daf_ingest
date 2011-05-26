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

from __future__ import with_statement

import errno
import getpass
import glob
import os, os.path
import optparse
import subprocess
import sys
from textwrap import dedent

from lsst.datarel.pmap import pmap
from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions


if not 'AP_DIR' in os.environ:
    print >>sys.stderr, "Please setup the ap package and try again"
    sys.exit(1)

AP_DIR = os.environ['AP_DIR']
cnvSource = os.path.join(AP_DIR, 'bin', 'boostPt1Source2CSV.py')
cnvObject = os.path.join(AP_DIR, 'bin', 'boostPt1Object2CSV.py')
refPosMatch = os.path.join(AP_DIR, 'bin', 'qa', 'refPosMatch.py')
refCcdFilter = os.path.join(AP_DIR, 'bin', 'qa', 'refCcdFilter.py')

def convert(kind, boostPath, csvPath):
    global cnvSource, cnvObject
    if kind == 'object':
        return subprocess.call(['python', cnvObject, boostPath, csvPath])
    else:
        return subprocess.call(['python', cnvSource, boostPath, csvPath])

def convertAll(root, scratch, numWorkers):
    kinds = ('badSource', 'source', 'object')
    tasks = []
    for kind in kinds:
        boostPaths = glob.glob(os.path.join(root, 'results', '*', kind + '.boost'))
        for boostPath in boostPaths:
            boostDir, boostFile = os.path.split(boostPath)
            skyTile = os.path.split(boostDir)[1]
            skyTileDir = os.path.join(scratch, skyTile)
            try:
                os.mkdir(skyTileDir, 0755)
            except OSError, ex:
                if ex.errno != errno.EEXIST:
                    raise
            csvFile = os.path.join(skyTileDir, os.path.splitext(boostFile)[0] + ".csv")
            tasks.append((kind, boostPath, csvFile))
    results = pmap(numWorkers, convert, tasks)
    for i, r in enumerate(results):
        if r != 0:
            print >>sys.stderr, "Failed to convert %s to %s" % (tasks[i][1], tasks[i][2])

def load(sql, scratch):
    for table, fileName in (('BadSource', 'badSource.csv'),
                            ('Source', 'source.csv')):
        for csv in glob.glob(os.path.join(scratch, '*', fileName)):
            sql.execStmt(dedent("""\
                LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s
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
                    modelFlux, modelFluxSigma,
                    petroFlux, petroFluxSigma,
                    instFlux, instFluxSigma,
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
                    flagForAssociation, flagForDetection, flagForWcs);
                """ % (os.path.abspath(csv), table)))
    for csv in glob.glob(os.path.join(scratch, '*', 'object.csv')):
        sql.execStmt(dedent("""\
            LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE Object
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
                earliestObsTime, latestObsTime,
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
                uFlux_SG, uFlux_SG_Sigma,
                uFlux_CSG, uFlux_CSG_Sigma,
                uTimescale, uEarliestObsTime, uLatestObsTime,
                uSersicN_SG, uSersicN_SG_Sigma,
                uE1_SG, uE1_SG_Sigma, uE2_SG, uE2_SG_Sigma,
                uRadius_SG, uRadius_SG_Sigma,
                uFlags,
                gNumObs, gExtendedness, gVarProb,
                gRaOffset_PS, gRaOffset_PS_Sigma,
                gDeclOffset_PS, gDeclOffset_PS_Sigma,
                gRaDeclOffset_PS_Cov,
                gRaOffset_SG, gRaOffset_SG_Sigma,
                gDeclOffset_SG, gDeclOffset_SG_Sigma,
                gRaDeclOffset_SG_Cov,
                gLnL_PS, gLnL_SG,
                gFlux_PS, gFlux_PS_Sigma,
                gFlux_SG, gFlux_SG_Sigma,
                gFlux_CSG, gFlux_CSG_Sigma,
                gTimescale, gEarliestObsTime, gLatestObsTime,
                gSersicN_SG, gSersicN_SG_Sigma,
                gE1_SG, gE1_SG_Sigma, gE2_SG, gE2_SG_Sigma,
                gRadius_SG, gRadius_SG_Sigma,
                gFlags,
                rNumObs, rExtendedness, rVarProb,
                rRaOffset_PS, rRaOffset_PS_Sigma,
                rDeclOffset_PS, rDeclOffset_PS_Sigma,
                rRaDeclOffset_PS_Cov,
                rRaOffset_SG, rRaOffset_SG_Sigma,
                rDeclOffset_SG, rDeclOffset_SG_Sigma,
                rRaDeclOffset_SG_Cov,
                rLnL_PS, rLnL_SG,
                rFlux_PS, rFlux_PS_Sigma,
                rFlux_SG, rFlux_SG_Sigma,
                rFlux_CSG, rFlux_CSG_Sigma,
                rTimescale, rEarliestObsTime, rLatestObsTime,
                rSersicN_SG, rSersicN_SG_Sigma,
                rE1_SG, rE1_SG_Sigma, rE2_SG, rE2_SG_Sigma,
                rRadius_SG, rRadius_SG_Sigma,
                rFlags,
                iNumObs, iExtendedness, iVarProb,
                iRaOffset_PS, iRaOffset_PS_Sigma,
                iDeclOffset_PS, iDeclOffset_PS_Sigma,
                iRaDeclOffset_PS_Cov,
                iRaOffset_SG, iRaOffset_SG_Sigma,
                iDeclOffset_SG, iDeclOffset_SG_Sigma,
                iRaDeclOffset_SG_Cov,
                iLnL_PS, iLnL_SG,
                iFlux_PS, iFlux_PS_Sigma,
                iFlux_SG, iFlux_SG_Sigma,
                iFlux_CSG, iFlux_CSG_Sigma,
                iTimescale, iEarliestObsTime, iLatestObsTime,
                iSersicN_SG, iSersicN_SG_Sigma,
                iE1_SG, iE1_SG_Sigma, iE2_SG, iE2_SG_Sigma,
                iRadius_SG, iRadius_SG_Sigma,
                iFlags,
                zNumObs, zExtendedness, zVarProb,
                zRaOffset_PS, zRaOffset_PS_Sigma,
                zDeclOffset_PS, zDeclOffset_PS_Sigma,
                zRaDeclOffset_PS_Cov,
                zRaOffset_SG, zRaOffset_SG_Sigma,
                zDeclOffset_SG, zDeclOffset_SG_Sigma,
                zRaDeclOffset_SG_Cov,
                zLnL_PS, zLnL_SG,
                zFlux_PS, zFlux_PS_Sigma,
                zFlux_SG, zFlux_SG_Sigma,
                zFlux_CSG, zFlux_CSG_Sigma,
                zTimescale, zEarliestObsTime, zLatestObsTime,
                zSersicN_SG, zSersicN_SG_Sigma,
                zE1_SG, zE1_SG_Sigma, zE2_SG, zE2_SG_Sigma,
                zRadius_SG, zRadius_SG_Sigma,
                zFlags,
                yNumObs, yExtendedness, yVarProb,
                yRaOffset_PS, yRaOffset_PS_Sigma,
                yDeclOffset_PS, yDeclOffset_PS_Sigma,
                yRaDeclOffset_PS_Cov,
                yRaOffset_SG, yRaOffset_SG_Sigma,
                yDeclOffset_SG, yDeclOffset_SG_Sigma,
                yRaDeclOffset_SG_Cov,
                yLnL_PS, yLnL_SG,
                yFlux_PS, yFlux_PS_Sigma,
                yFlux_SG, yFlux_SG_Sigma,
                yFlux_CSG, yFlux_CSG_Sigma,
                yTimescale, yEarliestObsTime, yLatestObsTime,
                ySersicN_SG, ySersicN_SG_Sigma,
                yE1_SG, yE1_SG_Sigma, yE2_SG, yE2_SG_Sigma,
                yRadius_SG, yRadius_SG_Sigma,
                yFlags);
            """ % os.path.abspath(csv)))

def referenceMatch(sql, root, scratch, refCatalog, radius, exposureMetadata=None):
    objectTsv = os.path.abspath(os.path.join(scratch, 'objDump.tsv'))
    sourceTsv = os.path.abspath(os.path.join(scratch, 'srcDump.tsv'))
    dumpPaf = os.path.abspath(os.path.join(scratch, 'dump.paf'))
    filtCsv = os.path.abspath(os.path.join(scratch, 'refFilt.csv'))
    objectMatchCsv = os.path.abspath(os.path.join(scratch, 'refObjMatch.csv'))
    sourceMatchCsv = os.path.abspath(os.path.join(scratch, 'refSrcMatch.csv'))
    # Filter reference catalog
    refCsvCols = ','.join(['refObjectId',
                           'isStar',
                           'varClass',
                           'ra',
                           'decl',
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
    filtArgs = ['python', refCcdFilter, refCatalog, filtCsv, root, '-F', refCsvCols]
    if exposureMetadata != None:
        if not hasattr(exposureMetadata, '__iter__'):
            exposureMetadata = [exposureMetadata]
        for f in exposureMetadata:
            filtArgs.append('-e')
            filtArgs.append(f)
    subprocess.call(filtArgs)
    # Dump object table
    with open(objectTsv, "wb") as f:
        sql.execStmt("""SET myisam_sort_buffer_size=1000000000;
                        SELECT o.objectId, o.ra_PS, o.decl_PS, AVG(s.taiMidPoint)
                        FROM Object AS o INNER JOIN Source AS s ON (s.objectId = o.objectId)
                        GROUP BY o.objectId
                        ORDER BY o.decl_PS""", f, ['-B', '-N'])
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
                     '-s', '-r', str(radius), '-P', dumpPaf, '-F', filtCsvCols])
    # Match reference objects to sources
    subprocess.call(['python', refPosMatch, filtCsv, sourceTsv, sourceMatchCsv,
                     '-r', str(radius), '-P', dumpPaf, '-F', filtCsvCols])

    # Load filtered reference catalog and matches
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE SimRefObject
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (%s);
                 """ % (filtCsv, filtCsvCols))
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE RefObjMatch
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                        refObjectId, objectId,
                        refRa, refDec, angSep,
                        nRefMatches, nObjMatches,
                        closestToRef, closestToObj,
                        flags);
                """ % objectMatchCsv)
    sql.execStmt("""LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE RefSrcMatch
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (
                        refObjectId, sourceId,
                        refRa, refDec, angSep,
                        nRefMatches, nSrcMatches,
                        closestToRef, closestToSrc,
                        flags);
                """ % sourceMatchCsv)

def main():
    # Setup command line options
    usage = dedent("""\
    usage: %prog [options] <database> <root> <scratch>

    Program which populates a mysql database with SourceAssoc results.

    <database>:   Name of database to create tables in. Note that the LSST
                  schema is assumed to have been loaded into this database
                  via prepareDb.py
    <root>:       Root directory containing pipeline results (boost
                  archives for sources and objects, and a pipeline output
                  registry). If CCD exposure metadata CSV files are not
                  available (these can be generated with ingestProcessed_*.py),
                  then this directory must also contain all calexp outputs
                  for the run.
    <scratch>:    A temporary directory used as scratch space when converting
                  boost persisted Source and SourceClusterAttributes vectors
                  to CSV form suitable for loading into MySQL.
    """)
    parser = optparse.OptionParser(usage)
    addDbOptions(parser)
    parser.add_option(
        "-j", "--num-workers", type="int", dest="numWorkers",
        default=4, help=dedent("""\
        Number of parallel job processes to split boost->csv conversion
        over."""))
    parser.add_option(
        "-m", "--match", action="store_true", dest="match",
        help=dedent("""\
        Turn on reference catalog to source cluster matching. This
        currently only works for LSST Sim runs."""))
    parser.add_option(
        "-R", "--ref-catalog", dest="refCatalog",
        default="/lsst/DC3/data/obs/ImSim/ref/simRefObject_12142010.csv",
        help="Reference catalog CSV file (%default).")
    parser.add_option(
        "-r", "--radius", type="float", dest="radius", default=2.0,
        help=dedent("""\
        Reference object to source cluster match radius, arcsec. The default
        is %default arcsec."""))
    parser.add_option(
        "-e", "--exposure-metadata", action="append", type="string",
        dest="exposureMetadata", help=dedent("""\
        The name of an exposure metadata key-value CSV table. This option
        may be specified more than once. If present, exposure metadata is
        obtained from the given CSV file(s) rather than the butler, and
        <root> need not contain calexp pipieline outputs."""))

    opts, args = parser.parse_args()
    if len(args) != 3 or not os.path.isdir(args[1]) or not os.path.isdir(args[2]):
        parser.error("A database name and root/scratch directories must be specified")
    if opts.user == None:
        parser.error("No database user name specified and $USER " +
                     "is undefined or empty")
    database, root, scratch = args
    sql = MysqlExecutor(opts.host, database, opts.user, opts.port)
    convertAll(root, scratch, opts.numWorkers)
    load(sql, scratch)
    if opts.match:
        referenceMatch(sql, root, scratch, opts.refCatalog, opts.radius,
                       opts.exposureMetadata)

if __name__ == "__main__":
    main()

