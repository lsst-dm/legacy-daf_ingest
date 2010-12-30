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

import os

from lsst.datarel import lsstSimMain, lsstSimSetup, runStage
from lsst.pex.logging import Log

import lsst.ap.cluster as apCluster

def getScienceCcdExposureId(visit, raft, sensor):
    r1, comma, r2 = raft
    s1, comma, s2 = sensor
    raftId = int(r1)*5 + int(r2)
    ccdNum = int(s1)*3 + int(s2)
    return (long(visit) << 9) + raftId*10 + ccdNum

def sourceAssocProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = lsstSimSetup(root, outRoot, registry,
            None, inButler, outButler)

    skyTile = keys['skyTile']
    srcList = []
    calexpMdList = []
    log = Log(Log.getDefaultLog(), "lsst.ap.cluster")
    for visit, raft, sensor in inButler.queryMetadata("raw", "sensor",
            ("visit", "raft", "sensor"), skyTile=skyTile):
        if inButler.datasetExists("src", visit=visit, raft=raft, sensor=sensor):
            srcs = inButler.get("src", visit=visit, raft=raft, sensor=sensor)
            # circumvent lazy-loading to make sure we can actually 
            # sources
            try:
                srcs.getSources()
                calexpMd = inButler.get("calexp_md",  visit=visit, raft=raft, sensor=sensor)
                calexpMd.setLong("scienceCcdExposureId",
                                 getScienceCcdExposureId(visit, raft, sensor))
                srcList.append(srcs)
                calexpMdList.append(calexpMd) 
            except:
                log.log(Log.WARN, "Failed to unpersist src or calexp_md for visit %s, R%s S%s" %
                        (str(visit), raft, sensor)) 
    if len(srcList) == 0:
        log.log(Log.WARN, "No sources found")
        return
    clip = sourceAssocPipe(srcList, calexpMdList, skyTile)

    if clip.contains('sources'):
        outButler.put(clip['sources'], 'source', **keys)
    if clip.contains('badSources'):
        outButler.put(clip['badSources'], 'badSource', **keys)
    if clip.contains('invalidSources'):
        outButler.put(clip['invalidSources'], 'invalidSource', **keys)
    if clip.contains('goodSourceHistogram'):
        outButler.put(clip['goodSourceHistogram'], 'sourceHist', **keys)
    if clip.contains('badSourceHistogram'):
        outButler.put(clip['badSourceHistogram'], 'badSourceHist', **keys)
    if clip.contains('sourceClusterAttributes'):
        outButler.put(clip['sourceClusterAttributes'], 'object', **keys)
    if clip.contains('badSourceClusterAttributes'):
        outButler.put(clip['badSourceClusterAttributes'], 'badObject', **keys)

def sourceAssocPipe(srcList, calexpMdList, skyTile):
    clip = {
        'inputSources': srcList,
        'inputExposures': calexpMdList,
        'jobIdentity': { 'skyTileId': skyTile },
    }

    clip = runStage(apCluster.SourceClusteringStage,
        """#<?cfg paf policy?>
        inputKeys: {
        }
        outputKeys: {
        }
        sourceClusteringPolicy: {
            epsilonArcsec: 0.5
            minNeighbors: 5
        }
        """, clip)

    clip = runStage(apCluster.SourceClusterAttributesStage,
        """#<?cfg paf policy?>
        inputKeys: {
        }
        outputKeys: {
        }
        """, clip)

    return clip

def run(root, skyTile):
    if os.path.exists(os.path.join(root, "registry.sqlite3")):
        registry = os.path.join(root, "registry.sqlite3")
    else:
        registry = "/lsst/DC3/data/obs/ImSim/registry.sqlite3"
    sourceAssocProcess(root, ".", registry, skyTile=skyTile)

def main():
    lsstSimMain(sourceAssocProcess, "source", "skyTile")

if __name__ == "__main__":
    main()
