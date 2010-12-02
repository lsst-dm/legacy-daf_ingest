#! /usr/bin/env python
import errno

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

import getpass
import glob
import os, os.path
import optparse
import subprocess
import sys
from textwrap import dedent
from lsst.datarel.pmap import pmap


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

def convertAll(inputRoot, outputRoot, numWorkers):
    kinds = ('badSource', 'source', 'object')
    tasks = []
    for kind in kinds:
        boostPaths = glob.glob(os.path.join(inputRoot, 'results', '*', kind + '.boost'))
        for boostPath in boostPaths:
            boostDir, boostFile = os.path.split(boostPath)
            skyTile = os.path.split(boostDir)[1]
            skyTileDir = os.path.join(outputRoot, skyTile)
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

def execStmt(stmt):
    global password, user, host
    print stmt
    sys.stdout.flush()
    subprocess.call(['mysql', '-h', host, '-u', user, '-p' + password,
                     '-e', stmt])

def setupDb(database, tableSuffix=""):
    execStmt("CREATE DATABASE IF NOT EXISTS %s;" % database)
    for tableName in ('BadSource', 'Source', 'Object', 'SimRefObject', 'RefObjMatch'):
        execStmt("CREATE TABLE %s.%s%s LIKE pt1_templates.%s" %
            (database, tableName, tableSuffix, tableName))

def fixupDb(database, tableSuffix=""):
    # Generate indexes, etc...
    for tableName in ('BadSource', 'Source'):
        execStmt("ALTER TABLE %s.%s%s ADD PRIMARY KEY (sourceId);" %
            (database, tableName, tableSuffix))
        execStmt("ALTER TABLE %s.%s%s ADD KEY (decl);" %
            (database, tableName, tableSuffix))
    execStmt("ALTER TABLE %s.Source%s ADD KEY (objectId);" % (database, tableSuffix))
    for filter in "ugrizy":
        execStmt("UPDATE %s.Object%s SET %sNumObs = 0 WHERE %sNumObs IS NULL;" %
            (database, tableSuffix, filter, filter))
    execStmt("ALTER TABLE %s.Object%s ADD PRIMARY KEY (objectId);" % (database, tableSuffix))
    execStmt("ALTER TABLE %s.Object%s ADD KEY (decl_PS);" % (database, tableSuffix))

def load(outputRoot, database, tableSuffix=""):
    for tableName, fileName in (('BadSource', 'badSource.csv'),
                                ('Source', 'source.csv'),
                                ('Object', 'object.csv')):
        for csv in glob.glob(os.path.join(outputRoot, '*', fileName)):
            execStmt("LOAD DATA INFILE '%s' INTO TABLE %s.%s%s FIELDS TERMINATED BY ',';" %
                (os.path.abspath(csv), database, tableName, tableSuffix))

def referenceMatch(inputRoot, outputRoot, database, refCatalog, radius, tableSuffix=""):
    objectCsv = os.path.abspath(os.path.join(outputRoot, 'objDump.csv'))
    filtCsv = os.path.abspath(os.path.join(outputRoot, 'refFilt.csv'))
    matchCsv = os.path.abspath(os.path.join(outputRoot, 'refObjMatch.csv'))
    # Filter reference catalog
    subprocess.call(['python', refCcdFilter, refCatalog, filtCsv, inputRoot,
                     '-F', 'refObjectId,isStar,ra,decl,gLat,gLon,sedName,' +
                     'uMag,gMag,rMag,iMag,zMag,yMag,muRa,muDecl,parallax,vRad,isVar,redshift'])
    # Dump object table
    execStmt("""SELECT o.objectId, o.ra_PS, o.decl_PS, AVG(s.taiMidPoint)
             FROM %s.Object%s AS o INNER JOIN %s.Source%s AS s ON (s.objectId = o.objectId)
             GROUP BY o.objectId
             ORDER BY o.decl_PS
             INTO OUTFILE '%s'
             FIELDS TERMINATED BY ',';
             """ % (database, tableSuffix, database, tableSuffix, objectCsv))
    # Match reference objects to objects
    subprocess.call(['python', refPosMatch, filtCsv, objectCsv, matchCsv,
                     '-s', '-r', str(radius), '-F', 'refObjectId,isStar,ra,decl,gLat,gLon,sedName,' +
                     'uMag,gMag,rMag,iMag,zMag,yMag,muRa,muDecl,parallax,vRad,isVar,redshift,' +
                     'uCov,gCov,rCov,iCov,zCov,yCov', '-f', 'objectId,ra,dec,epoch'])
    # Load filtered reference catalog and matches
    execStmt("""LOAD DATA INFILE '%s' INTO TABLE %s.SimRefObject%s
             FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"';""" %
             (filtCsv, database, tableSuffix))
    execStmt("""LOAD DATA INFILE '%s' INTO TABLE %s.RefObjMatch%s
             FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"';""" %
             (matchCsv, database, tableSuffix))
    execStmt("ALTER TABLE %s.SimRefObject%s ADD PRIMARY KEY (refObjectId);" % (database, tableSuffix))
    execStmt("ALTER TABLE %s.SimRefObject%s ADD KEY (decl);" % (database, tableSuffix))
    execStmt("ALTER TABLE %s.RefObjMatch%s ADD KEY (refObjectId);" % (database, tableSuffix))
    execStmt("ALTER TABLE %s.RefObjMatch%s ADD KEY (objectId);" % (database, tableSuffix))


def main():
    global user, password, host
    defuser = (os.environ.has_key('USER') and os.environ['USER']) or "serge"
    
    # Setup command line options
    usage = dedent("""\
    usage: %prog [options] <database> <inputRoot> <outputRoot>

    Program which populates a mysql database with SourceAssoc results.

    <database>:   Name of database to create tables in.
    <inputRoot>:  Root directory containing SourceAssoc results (boost
                  archives for sources and objects)
    <outputRoot>: Directory to store CSV files in.
    """)
    parser = optparse.OptionParser(usage)
    parser.add_option(
        "-u", "--user", dest="user", default=defuser, help=dedent("""\
        Database user name to use when connecting to MySQL servers."""))
    parser.add_option(
        "-H", "--dbhost", dest="host", default="lsst10.ncsa.uiuc.edu",
        help=dedent("""\
        Database user name to use when connecting to MySQL servers."""))
    parser.add_option(
        "-s", "--suffix", dest="suffix", default="",
        help=dedent("""\
        Specifies a table name suffix to append to the standard table
        names (BadSource, Source, Object, ...)."""))
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
        default="/lsst/DC3/data/obs/ImSim/ref/simRefObject_1032010.csv",
        help="Reference catalog CSV file (%default).")
    parser.add_option(
        "-r", "--radius", type="float", dest="radius", default=2.0,
        help=dedent("""\
        Reference object to source cluster match radius, arcsec. The default
        is %default arcsec."""))
    opts, args = parser.parse_args()
    if len(args) != 3 or not os.path.isdir(args[1]) or not os.path.isdir(args[2]):
        parser.error("A database name and input/output directories must be specified")
    host = opts.host
    user = opts.user
    password = getpass.getpass("%s's MySQL password: " % user)
    database, inputRoot, outputRoot = args
    setupDb(database, opts.suffix)
    convertAll(inputRoot, outputRoot, opts.numWorkers)
    load(outputRoot, database, opts.suffix)
    fixupDb(database, opts.suffix)
    if opts.match:
        referenceMatch(inputRoot, outputRoot, database,
                       opts.refCatalog, opts.radius, opts.suffix)

if __name__ == "__main__":
    main()

