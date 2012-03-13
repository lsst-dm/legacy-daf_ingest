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
from itertools import izip
import os, os.path
import re
import shlex
import sys

import lsst.daf.persistence as dafPersist
from .mysqlExecutor import addDbOptions


__all__ = ["makeArgumentParser",
           "visitLsstSimCalexps",
           "visitCfhtCalexps",
           "visitSkyTiles",
          ]

def _line_to_args(self, line):
    for arg in shlex.split(line, comments=True, posix=True):
        if not arg.strip():
            continue
        yield arg

class _IdAction(argparse.Action):
    """Parse a data ID specficiation
    """
    def __call__(self, parser, namespace, values, option_string):
        """Parse --id value and append results to namespace.dataId"""
        id = dict()
        for kv in values:
            k, sep, v = kv.partition("=")
            k = k.strip()
            v = [x.strip() for x in v.split("^")]
            if any(len(x) == 0 for x in v):
                raise RuntimeError('Invalid key=val pair in --id ("' + kv + '")')
            if k in id:
                id[k].update(v)
            else:
                id[k] = set(v)
        if hasattr(namespace, "dataId"):
            namespace.dataId.append(id)
        else:
            namespace.dataId = [id]

def _searchRules(namespace, dataset, keySpecs):
    """Ensures that namespace.dataId contains only data ID specs
    that involve the given keys (ordered by level), and that each
    value conforms to the given regular expression.
    """
    if not hasattr(namespace, "dataId"):
        return [[None]*len(keySpecs)]
    rules = []
    keySet = set(ktr[0] for ktr in keySpecs)
    for id in namespace.dataId:
        if any(k not in keySet for k in id):
            raise RuntimeError(k + " is not a valid data ID key for " +
                               dataset + " data")
        r = []
        for k, typ, pat in keySpecs:
            if k not in id:
                r.append(None)
                continue
            if any(re.match(pat, v) is None for v in id[k]):
                raise RuntimeError(v + " is not a valid " + k +
                                   " key value for " + dataset + " data")
            r.append(set(typ(v) for v in id[k]))
        rules.append(r)
    return rules


def makeArgumentParser(description, inRootsRequired=True, addRegistryOption=True):
    parser = argparse.ArgumentParser(
        description=description,
        fromfile_prefix_chars="@",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Data IDs are expected to be of the form:\n"
               "  --id k1=v11[^v12[...]] [k2=v21[^v22[...]]\n"
               "\n"
               "Examples:\n"
               "  1. --id visit=12345 raft=1,1\n"
               "  2. --id skyTile=12345\n"
               "  3. --id visit=12 raft=1,2^2,2 sensor=1,1^1,3\n"
               "\n"
               "The first example identifies all sensors in raft 1,1 of visit "
               "12345. The second identifies all sources/objects in sky-tile "
               "12345. The cross product is computed for keys with multiple "
               "values, so the third example is equivalent to:\n"
               "  --id visit=12 raft=1,2 sensor=1,1\n"
               "  --id visit=12 raft=1,2 sensor=1,3\n"
               "  --id visit=12 raft=2,2 sensor=1,1\n"
               "  --id visit=12 raft=2,2 sensor=1,3\n"
               "\n"
               "Redundant specification of a data ID will *not* result in "
               "loads of duplicate data - data IDs are de-duped before ingest "
               "starts. Note also that the keys allowed in data IDs are "
               "specific to the type of data the ingestion script deals with. "
               "For example, one cannot load sensor metadata by sky tile ID, nor "
               "sources by sensor (CCD) ID.\n"
               "\n"
               "Any omitted keys are assumed to take all legal values. So, "
               "`--id raft=1,2` identifies all sensors of raft 1,2 for all "
               "available visits.\n"
               "\n"
               "Finally, calling an ingestion script multiple times is safe. "
               "Attempting to load the same data item twice will result "
               "in an error or (if --strict is specified) or cause previously "
               "loaded data item to be skipped. Database activity is strictly "
               "append only.")
    parser.convert_arg_line_to_args = _line_to_args
    addDbOptions(parser)
    parser.add_argument(
        "-d", "--database", dest="database",
        help="MySQL database to load CSV files into.")
    parser.add_argument(  
        "-s", "--strict", dest="strict", action="store_true",
        help="Error out if previously ingested, incomplete, or otherwise "
             "suspicious data items are encountered (by default, these are "
             "skipped). Note that if --database is not supplied, detecting "
             "previously ingested data is not possible.")
    parser.add_argument(
        "-i", "--id", dest="id", nargs="*", action=_IdAction,
        help="Data ID specifying what to ingest. May appear multiple times.")
    if addRegistryOption:
        parser.add_argument(
            "-R", "--registry", dest="registry", help="Input registry path; "
            "used for all input roots. If omitted, a file named registry.sqlite3 "
            "must exist in each input root.")
    parser.add_argument(
        "outroot", help="Output directory for CSV files")
    parser.add_argument(
        "inroot", nargs="+" if inRootsRequired else "*",
        help="One or more input root directories")
    return parser


class _Butler(object):
    """Instances of _Butler create (and cache) a single data butler. Used to
    avoid creating a data buter (expensive) if a root directory does not
    contain any data matching a data ID spec.
    """
    def __init__(self, root, namespace, mapperClass):
        self._root = root
        self._ns = namespace
        self._cls = mapperClass
        self._butler = None

    def __call__(self):
        if self._butler == None:
            registry = self._ns.registry or os.path.join(self._root, "registry.sqlite3")
            mapper = self._cls(root=self._root, registry=registry)
            self._butler = dafPersist.ButlerFactory(mapper=mapper).create()
        return self._butler


def _getVisits(dir):
    paths = os.listdir(os.path.join(dir, 'calexp'))
    visits = dict()
    for p in paths:
        m = re.match(r"^v(\d+)-f.*", p)
        if m is not None and os.path.isdir(os.path.join(dir, 'calexp', p)):
            visits[int(m.group(1))] = p
    return visits, set(visits.keys());

def _getSkyTiles(dir):
    paths = os.listdir(os.path.join(dir, 'results'))
    skyTiles = dict()
    for p in paths:
        m = re.match(r"^st(\d+)$", p)
        if m is not None and os.path.isdir(os.path.join(dir, 'results', p)):
            skyTiles[int(m.group(1))] = p
    return skyTiles, set(skyTiles.keys());

def _checkDisjoint(dirs, idSets, descr):
    assert len(dirs) == len(idSets)
    if len(dirs) > 1:
        for i in xrange(1, len(dirs)):
            for j in xrange(i):
                if not idSets[j].isdisjoint(idSets[i]):
                    raise RuntimeError(str.format(
                         "{} present in input roots {} and {} overlap!",
                         descr, dirs[j], dirs[i]))


_rafts = [       "0,1", "0,2", "0,3",
          "1,0", "1,1", "1,2", "1,3", "1,4",
          "2,0", "2,1", "2,2", "2,3", "2,4",
          "3,0", "3,1", "3,2", "3,3", "3,4",
                 "4,1", "4,2", "4,3"]

def visitLsstSimCalexps(namespace, processFunc, sql=None):
    """[obs_lsstSim] Invokes processFunc on the calibrated exposures in one more input
    directories that match an optional set of data ID specifications and
    which have not already been loaded into the target database.

    Arguments of processFunc are expected to be (in order):

    butler:         A butler for the input root the calibrated
                    exposure belongs to
    path:           Path to the calibrated exposure
    sciCcdExpId:    ID of calibrated exposure
    visit:          visit ID of calibrated exposure
    raft:           raft name ('X,Y') of calibrated exposure
    raftNum:        raft number
    sensor:         sensor name ('X,Y') of calibrated exposure
    sensorNum:      sensor number
    """
    import lsst.obs.lsstSim
    rules = _searchRules(namespace, "calexp",
                         [("visit", int, r"^\d+$"),
                          ("raft", str, r"^\d,\d$"),
                          ("sensor", str, r"^\d,\d$")])
    dirs = set(os.path.realpath(d) for d in namespace.inroot)
    for d in dirs:
        if not os.path.isdir(os.path.join(d, "calexp")):
            msg = str.format("invalid input dir {} : no 'calexp/' subdir", d)
            if not namespace.strict:
                print >>sys.stderr, "*** Skipping " + msg
                dirs.remove(d)
            else:
                raise RuntimeError(msg)
    visitDicts, visitSets = zip(*[_getVisits(d) for d in dirs])
    # make sure visits in each input root are disjoint
    _checkDisjoint(dirs, visitSets, "Visits")

    # Figure out what's been loaded already.
    # For current run sizes, this should hopefully be OK...
    if sql:
        loaded = set(r[0] for r in sql.runQuery(
            "SELECT scienceCcdExposureId from Science_Ccd_Exposure"))
    else:
        loaded = set()
 
    for rootDir, visitDict, visitSet in izip(dirs, visitDicts, visitSets):
        butler = _Butler(rootDir, namespace, lsst.obs.lsstSim.LsstSimMapper)
        for visit in visitSet:
            raftRules = []
            for r in rules:
                if r[0] is None or visit in r[0]:
                    raftRules.append(r)
            if len(raftRules) == 0:
                # no rules match visit
                continue
            visitDir = os.path.join(rootDir, 'calexp', visitDict[visit])
            # obtain listing of raft directories
            raftDirs = os.listdir(visitDir)
            for raftDir in raftDirs:
                m = re.match(r"R(\d\d)", raftDir)
                if m is None:
                    continue
                raftDir = os.path.join(visitDir, raftDir)
                if not os.path.isdir(raftDir):
                    continue
                raft = ",".join(m.group(1))
                sensorRules = []
                for r in raftRules:
                    if r[1] is None or raft in r[1]:
                        sensorRules.append(r)
                if len(sensorRules) == 0:
                    # no rules match raft
                    continue
                raftNum = _rafts.index(raft)
                if raftNum == -1:
                    raise RuntimeError("Unable to map raft name " + raft +
                                       " to raft number")
                r1, _, r2 = raft
                raftId = int(r1) * 5 + int(r2)
                # obtain listing of sensors
                sensorFiles = os.listdir(raftDir)
                for sensorFile in sensorFiles:
                    m = re.match(r"S(\d\d).fits", sensorFile)
                    if m is None:
                        continue
                    sensor = ",".join(m.group(1))
                    if not any(r[2] is None or sensor in r[2] for r in sensorRules):
                        continue
                    s1, _, s2 = sensor
                    sensorNum = int(s1) * 3 + int(s2)
                    sciCcdExpId = (long(visit) << 9) + raftId * 10 + sensorNum
                    if sciCcdExpId in loaded:
                        msg = str.format(" visit {} raft {} sensor {} : already ingested",
                                         visit, raft, sensor)
                        if not namespace.strict:
                            print >>sys.stderr, "*** Skipping " + msg
                            continue
                        else:
                            raise RuntimeError(msg)
                    processFunc(butler(),
                                os.path.join(raftDir, sensorFile),
                                sciCcdExpId,
                                visit,
                                raft,
                                raftNum,
                                sensor,
                                sensorNum) 


def visitCfhtCalexps(namespace, processFunc, sql=None):
    """[obs_cfht] Invokes processFunc on the calibrated exposures in one more input
    directories that match an optional set of data ID specifications and
    which have not already been loaded into the target database.

    Arguments of processFunc are expected to be (in order):

    butler:         A butler for the input root the calibrated
                    exposure belongs to
    path:           Path to the calibrated exposure
    sciCcdExpId:    ID of calibrated exposure
    visit:          visit ID of calibrated exposure
    ccd:            ccd ID of calibrated exposure
    """
    import lsst.obs.cfht
    rules = _searchRules(namespace, "calexp",
                         [("visit", int, r"^\d+$"),
                          ("ccd", int, r"^\d\d$")])
    dirs = set(os.path.realpath(d) for d in namespace.inroot)
    for d in dirs:
        if not os.path.isdir(os.path.join(d, "calexp")):
            msg = str.format("invalid input dir {} : no 'calexp/' subdir", d)
            if not namespace.strict:
                print >>sys.stderr, "*** Skipping " + msg
                dirs.remove(d)
            else:
                raise RuntimeError(msg)

    visitDicts, visitSets = zip(*[_getVisits(d) for d in dirs])
    # make sure visits in each input root are disjoint
    _checkDisjoint(dirs, visitSets, "Visits")

    # Figure out what's been loaded already.
    # For current run sizes, this should hopefully be OK...
    if sql:
        loaded = set(r[0] for r in sql.runQuery(
            "SELECT scienceCcdExposureId from Science_Ccd_Exposure"))
    else:
        loaded = set()

    for rootDir, visitDict, visitSet in izip(dirs, visitDicts, visitSets):
        butler = _Butler(rootDir, namespace, lsst.obs.cfht.CfhtMapper)
        for visit in visitSet:
            ccdRules = []
            for r in rules:
                if r[0] is None or visit in r[0]:
                    ccdRules.append(r)
            if len(ccdRules) == 0:
                # no rules match visit
                continue
            visitDir = os.path.join(rootDir, 'calexp', visitDict[visit])
            # obtain listing of raft directories
            ccdFiles= os.listdir(visitDir)
            for ccdFile in ccdFiles:
                m = re.match(r"c(\d\d).fits", ccdFile)
                if m is None:
                    continue
                ccd = int(m.group(1))
                if not any(r[1] is None or ccd in r[1] for r in ccdRules):
                    continue
                sciCcdExpId = (long(visit) << 6) + ccd

                if sciCcdExpId in loaded:
                    msg = str.format("visit {} ccd {} : already ingested",
                                     visit, ccd)
                    if not namespace.strict:
                        print >>sys.stderr, "*** Skipping " + msg
                        continue
                    else:
                        raise RuntimeError(msg)
                processFunc(butler(),
                            os.path.join(visitDir, ccdFile),
                            sciCcdExpId,
                            visit,
                            ccd)


def visitSkyTiles(namespace, sql=None):
    """A generator over the sky-tiles in one more input directories
    that match an optional set of data ID specifications and which have
    not already been loaded into the target database.

    Tuples with the following elements are yielded:

    root:           Name of sky-tile parent directory
    skyTileDir:     Sky-tile directory name (relative to parent)
    skyTileId:      Sky-tile ID
    """
    rules = _searchRules(namespace, "SourceAssoc output",
                         [("skyTile", int, r"^\d+$")])
    dirs = set(os.path.realpath(d) for d in namespace.inroot)
    for d in dirs:
        if not os.path.isdir(os.path.join(d, "results")):
            msg = str.format("invalid input dir {} : no 'results/' subdir", d)
            if not namespace.strict:
                print >>sys.stderr, "*** Skipping " + msg
                dirs.remove(d)
            else:
                raise RuntimeError(msg)

    stDicts, stSets = zip(*[_getSkyTiles(d) for d in dirs])
    # make sure visits in each input root are disjoint
    _checkDisjoint(dirs, stSets, "Sky-tiles")

    # Figure out what's been loaded already.
    if sql:
        loaded = set(r[0] for r in sql.runQuery("SELECT skyTileId from SkyTile"))
    else:
        loaded = set()

    for rootDir, stDict, stSet in izip(dirs, stDicts, stSets):
        rootDir = os.path.join(rootDir, "results")
        for skyTile in stSet:
            if not any(r[0] is None or skyTile in r[0] for r in rules):
                continue
            if skyTile in loaded:
                msg = str.format("sky-tile {} : already ingested", skyTile)
                if not namespace.strict:
                    print >>sys.stderr, "*** Skipping " + msg
                    continue
                else:
                    raise RuntimeError(msg)
            yield (rootDir, stDict[skyTile], skyTile)


def pruneSkyTileDirs(namespace, stDirs):
    """Prunes a list of sky-tile directories according to sky-tile data ID specs.
    """
    stDirIdPairs = []
    rules = _searchRules(namespace, "SourceAssoc output",
                         [("skyTile", int, r"^\d+$")])
    for d in stDirs:
        skyTile = int(re.match(r".*/st(\d+)/?$", d).group(1))
        if not any(r[0] is None or skyTile in r[0] for r in rules):
            continue
        stDirIdPairs.append((d, skyTile))
    return stDirIdPairs

