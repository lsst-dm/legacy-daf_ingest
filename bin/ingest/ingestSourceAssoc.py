#! /usr/bin/env python
import errno
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
    global password, user
    print stmt
    sys.stdout.flush()
    subprocess.call(['mysql', '-u', user, '-p' + password, '-e', stmt])

def setupDb(database, tableSuffix=""):
    execStmt("CREATE DATABASE IF NOT EXISTS %s;" % database)
    for tableName in ('BadSource', 'Source', 'Object'):
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

def main():
    global user, password
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
        "-u", "--user", dest="user", default="serge", help=dedent("""\
        Database user name to use when connecting to MySQL servers."""))
    parser.add_option(
        "-s", "--suffix", dest="suffix", default="",
        help=dedent("""\
        Specifies a table name suffix to append to the standard table
        names (BadSource, Source, Object)."""))
    parser.add_option(
        "-j", "--num-workers", type="int", dest="numWorkers",
        default=4, help=dedent("""\
        Number of parallel job processes to split boost->csv conversion
        over."""))
    opts, args = parser.parse_args()
    if len(args) != 3 or not os.path.isdir(args[1]) or not os.path.isdir(args[2]):
        parser.error("A database name and input/output directories must be specified")
    user = opts.user
    password = getpass.getpass()
    database, inputRoot, outputRoot = args
    setupDb(database, opts.suffix)
    convertAll(inputRoot, outputRoot, opts.numWorkers)
    load(outputRoot, database, opts.suffix)
    fixupDb(database, opts.suffix)

if __name__ == "__main__":
    main()

