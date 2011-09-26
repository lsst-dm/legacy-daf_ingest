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

"""Recreate a calexp (or other) dataset based on provenance.
    Or create a dataset using the pipeline policies in the datarel package.

    -h for command line help.

    An appropriate EUPS_PATH and EUPS_DIR equivalent to that used
    during the run being reproduced must be set.

    recreateOutputs() takes a run directory (the directory from an Orca-based
    run containing the input, update, and work subdirectories), a list of
    outputs to be produced, a visit number, raft id (in "x,y" form like
    "1,2"), and a sensor id (also in "x,y" form).  The output directory may
    also be specified.

    Outputs are either specified by providing a dataset type (in which case
    all outputs of that type are produced) or by providing a stage name and
    clipboard item as a dotted pair (in which case that output is produced).
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
            usage="""%prog [-l] [-c] [-u] [-s] [-d OUTDIR] [-o OUTPUT [-o OUTPUT] ...] RUNDIR VISIT RAFT SENSOR
            
Recreate a calexp (or other) dataset based on provenance.
Or create a dataset using the pipeline policies in the datarel package (with
-u and usually -s).

An appropriate EUPS_PATH and EUPS_DIR equivalent to that used
during the run being reproduced must be set.

The required parameters are a run directory (the directory from an Orca-based
run containing the input, update, and work subdirectories) a visit number, a
raft id (in "x,y" form like "1,2"), and a sensor id (also in "x,y" form).""")

    parser.add_option("-l", "--list", action="store_true",
            help="list available outputs")
    parser.add_option("-o", "--output", action="append",
            help="select an output or dataset type")
    parser.add_option("-c", "--compare", action="store_true",
            help="compare result with previously computed dataset(s)")
    parser.add_option("-d", "--dir", default=".",
            help='output directory (default="%default")')
    parser.add_option("-u", "--useDatarel", action="store_true",
            help='use datarel policy instead of run policy; uses run directory only for inputs')
    parser.add_option("-s", "--stack", action="store_true",
            help="use currently setup stack instead of provenance-based")
    
    options, args = parser.parse_args()
    
    if (options.list and len(args) < 1) or \
            (not options.list and len(args) != 4):
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
    
def prepLocation(inputRoot, outputRoot):
    """Prepare persistence with the input and output directories.
    
    @param inputRoot (string) input root directory
    @param outputRoot (string) output root directory"""

    import lsst.daf.base as dafBase
    import lsst.daf.persistence as dafPersist

    locMap = dafBase.PropertySet()
    locMap.set("input", inputRoot)
    locMap.set("update", outputRoot)
    outRegistry = os.path.join(outputRoot, "registry.sqlite3")
    if outputRoot != inputRoot:
        if os.path.exists(outRegistry):
            os.unlink(outRegistry)
        os.symlink(os.path.join(inputRoot, "registry.sqlite3"), outRegistry)
    dafPersist.LogicalLocation.setLocationMap(locMap)

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

def recreateOutputs(runDir, outputList, visit, raft, sensor,
        outputDir=".", useDatarel=False, compare=False):
    """Recreate dataset(s) based on provenance.

    @param runDir (string) run directory
    @param outputList (list of strings) 
    @param visit (int) visit number
    @param raft (string) raft id "x,y"
    @param sensor (string) sensor id "x,y"
    @param outputDir (string) output directory
    @param compare (bool) compare with the dataset(s) in the run directory?"""

    from lsst.pex.harness.simpleStageTester import SimpleStageTester

    if not useDatarel:
        checkStack(runDir)
        inputRoot = os.path.join(runDir, "input")
    else:
        inputRoot = runDir

    prepLocation(inputRoot, outputDir)

    if outputList is None:
        outputList = ["calexp"]
    pol = loadMasterPolicy(runDir, useDatarel)
    outputs, datasetTypes = findOutputs(pol)
    outputStages = set()
    for o in outputList:
        if datasetTypes.has_key(o):
            outputStages.update(datasetTypes[o])
        elif o in outputs:
            outputStages.add(o.split('.')[0])

    sst = SimpleStageTester()
    for stagePolicy in pol.getPolicyArray("execute.appStage"):
        stageName = stagePolicy.getString("name")
        if stageName in ["getAJob", "jobDone"]:
            continue
        if isOutputStage(stagePolicy):
            if stageName in outputStages:
                stageOutputs = stagePolicy.getPolicy(
                        "stagePolicy.parameters.outputItems")
                outputNames = stageOutputs.policyNames(True)
                for o in outputNames:
                    datasetType = \
                            stageOutputs.getString(o + ".datasetId.datasetType")
                    if stageName + "." + o not in outputList and \
                            datasetType not in outputList:
                        stagePolicy.remove(
                                "stagePolicy.parameters.outputItems." + o)
                importAndAddStage(sst, stagePolicy)
                outputStages.remove(stageName)
                if len(outputStages) == 0:
                    break
        else:
            importAndAddStage(sst, stagePolicy)

    jobIdentity = dict(visit=visit, raft=raft, sensor=sensor)
    clip = sst.runWorker(dict(jobIdentity=jobIdentity))

def loadMasterPolicy(runDir, useDatarel=False):
    """Load the master policy file and its subsidiary files.

    @param runDir (string) run directory"""

    import lsst.pex.policy as pexPolicy

    if useDatarel:
        pipeDir = os.path.join(os.environ['DATAREL_DIR'], "pipeline")
        masterPolicy = os.path.join(pipeDir, "PT1Pipe", "main-ImSim.paf")
        return pexPolicy.Policy.createPolicy(masterPolicy, pipeDir)
    masterPolicy = os.path.join(runDir, "work", "main-ImSim.paf")
    return pexPolicy.Policy.createPolicy(masterPolicy)

def isOutputStage(pol):
    return pol.getString("parallelClass") == \
            "lsst.pex.harness.IOStage.OutputStageParallel"

def findOutputs(pol):
    outputs = {}
    datasetTypes = {}
    for stagePolicy in pol.getPolicyArray("execute.appStage"):
        if not isOutputStage(stagePolicy):
            continue

        stageName = stagePolicy.getString("name")
        stageOutputs = stagePolicy.getPolicy(
                "stagePolicy.parameters.outputItems")
        outputNames = stageOutputs.policyNames(True)
        for name in outputNames:
            datasetType = \
                    stageOutputs.getString(name + ".datasetId.datasetType")
            outputs[stageName + "." + name] = datasetType
            if not datasetTypes.has_key(datasetType):
                datasetTypes[datasetType] = set()
            datasetTypes[datasetType].add(stageName)
    return outputs, datasetTypes

def main():
    """Main program"""

    options, args = parseOptions()

    if not options.stack:
        setupStack(args[0])
        ret = subprocess.call(["python"] + sys.argv + ["-s"])
        sys.exit(ret)

    if options.list:
        masterPolicy = loadMasterPolicy(args[0], options.useDatarel)
        outputs, datasetTypes = findOutputs(masterPolicy)
        print "Available dataset types:"
        dsTypes = datasetTypes.keys()
        dsTypes.sort()
        for t in dsTypes:
            print "\t" + t
        print "Available outputs:"
        outputList = outputs.keys()
        outputList.sort()
        for o in outputList:
            print "\t%s (%s)" % (o, outputs[o])
        sys.exit(0)

    (runDir, visit, raft, sensor) = args
    recreateOutputs(runDir, options.output, int(visit), raft, sensor,
            options.dir, options.useDatarel, options.compare)

if __name__ == "__main__":
    main()
