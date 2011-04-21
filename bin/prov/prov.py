#!/usr/bin/env python

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

"""Recreate a calexp dataset based on provenance.

    -h for command line help.

    recreateCalexp() takes a run directory (the directory from an Orca-based
    run containing the input, update, and work subdirectories), a visit
    number, raft id (in "x,y" form like "1,2"), and a sensor id (also in "x,y"
    form).  The output filename for the calexp dataset may also be specified.
    If compareCalexp is True, the output dataset will be compared against the
    one in the run directory to ensure that they are identical.
"""

from __future__ import with_statement

from optparse import OptionParser
import os
import re
import subprocess
import sys

import eups

def parseOptions():
    """Parse the command line options."""

    parser = OptionParser(
            usage="%prog [-c] [-o OUTPUT] RUNDIR VISIT RAFT SENSOR")
    parser.add_option("-c", "--calexp", action="store_true",
            help="compare result with previously computed calexp")
    parser.add_option("-o", "--output", default="calexp.fits",
            help='output filename (default="%default")')
    parser.add_option("-s", "--stack", action="store_true",
            help="(internal) use currently setup stack instead of provenance-based")
    
    options, args = parser.parse_args()
    
    if len(args) != 4:
        parser.error("incorrect number of arguments")
    
    return options, args

def setupStack(runDir):
    """Use eups to setup the saved stack configuration.
    
    @param runDir (string) run directory"""

    e = eups.Eups(readCache=False)
    with open(os.path.join(runDir, "config", "weekly.tags")) as f:
        for line in f:
            pkg, ver = line.split(None, 2)[0:2]
            if pkg == 'eups' or pkg == 'lsst':
                continue
            ok, version, reason = e.setup(pkg, ver, noRecursion=True)
            if not ok or version != ver:
                raise RuntimeError, \
                        "Unable to setup version %s due to %s" % (ver, reason)

def checkStack(runDir):
    """Check that the stack configuration matches the provenance.
    
    @param runDir (string) run directory"""

    e = eups.Eups(readCache=False)
    with open(os.path.join(runDir, "config", "weekly.tags")) as f:
        for line in f:
            pkg, ver = line.split(None, 2)[0:2]
            version = e.findSetupVersion(pkg)[0]
            if version != ver:
                print >>sys.stderr, "*** WARNING:", pkg, "supposed to be", \
                        ver, "but got", version
    
def prepLocation(inputRoot):
    """Prepare persistence with the input directory.
    
    @param inputRoot (string) input root directory"""

    import lsst.daf.base as dafBase
    import lsst.daf.persistence as dafPersist

    locMap = dafBase.PropertySet()
    locMap.set("input", inputRoot)
    dafPersist.LogicalLocation.setLocationMap(locMap)

def includeStage(stagePolicy):
    """Determine if a stage should be included in the pipeline.

    @param stagePolicy (pexPolicy.Policy) appStage policy for the stage"""

    stageName = stagePolicy.getString("name")
    return stageName not in \
            ["getAJob", "jobDone"] \
            and not stageName.startswith("sfm") \
            and stageName.find("Output") == -1

def importAndAddStage(sst, stagePolicy):
    """Create a stage object and add it to the pipeline.

    @param[inout] sst (SimpleStageTester) the pipeline being constructed
    @param stagePolicy (pexPolicy.Policy) appStage policy for the stage"""

    stageName = stagePolicy.getString("name")
    stageClass = stagePolicy.getString("parallelClass")
    stageClass = re.sub(r'Parallel$', '', stageClass)
    if not stageClass.endswith("Stage"):
        stageClass += "Stage"
    tokenList = stageClass.split('.')
    importClassString = tokenList.pop().strip()
    importPackage = ".".join(tokenList)
    module = __import__(importPackage, globals(), locals(), \
            [importClassString], -1)
    if not hasattr(module, importClassString):
        raise TypeError, "Unable to find stage class %s in module %s" % (
                importClassString, importPackage)
    stage = getattr(module, importClassString)
    sst.addStage(stage(stagePolicy.getPolicy("stagePolicy")), stageName)

def recreateCalexp(runDir, visit, raft, sensor,
        output="calexp.fits", compareCalexp=False):
    """Recreate a calexp dataset based on provenance.

    @param runDir (string) run directory
    @param visit (int) visit number
    @param raft (string) raft id "x,y"
    @param sensor (string) sensor id "x,y"
    @param output (string) output filename
    @param compareCalexp (bool) compare with the calexp in the run directory?"""

    from lsst.pex.harness.simpleStageTester import SimpleStageTester
    import lsst.pex.policy as pexPolicy

    checkStack(runDir)

    inputRoot = os.path.join(runDir, "input")
    workDir = os.path.join(runDir, "work")
    masterPolicy = os.path.join(workDir, "main-ImSim.paf")
    pol = pexPolicy.Policy.createPolicy(masterPolicy)
    prepLocation(inputRoot)

    sst = SimpleStageTester()
    for stagePolicy in pol.getPolicyArray("execute.appStage"):
        if includeStage(stagePolicy):
            importAndAddStage(sst, stagePolicy)
    jobIdentity = dict(visit=visit, raft=raft, sensor=sensor)
    clip = sst.runWorker(dict(jobIdentity=jobIdentity))
    clip.get("scienceExposure").writeFits(output)

    if compareCalexp:
        path = os.path.join("calexp", "v"+str(visit)+"-f*",
                "R"+raft[0]+raft[2], "S"+sensor[0]+sensor[2]+".fits")
        ret = subprocess.call("cmp " +
                os.path.join(runDir, "update", path) + " " + output,
                shell=True)
        if ret == 0:
            print >>sys.stderr, "Comparison succeeded"

def main():
    """Main program"""

    options, (runDir, visit, raft, sensor) = parseOptions()

    if not options.stack:
        setupStack(runDir)
        ret = subprocess.call(sys.argv + ["-s"])
        sys.exit(ret)

    recreateCalexp(runDir, int(visit), raft, sensor,
            options.output, options.calexp)

if __name__ == "__main__":
    main()
