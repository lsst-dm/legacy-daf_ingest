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


from lsst.datarel import lsstSimMain, lsstSimSetup, runStage

import lsst.ap.cluster as apCluster

def sourceAssocProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = lsstSimSetup(root, outRoot, registry,
            None, inButler, outButler)

    skyTile = keys['skyTile']
    srcList = []
    for visit, raft, sensor in inButler.queryMetadata("raw", "sensor",
            ("visit", "raft", "sensor"), skyTile=skyTile):
        if inButler.datasetExists("src", visit=visit, raft=raft, sensor=sensor):
            srcs = inButler.get("src", visit=visit, raft=raft, sensor=sensor)
            srcList.append(srcs)
    if len(srcList) == 0:
        raise RuntimeError("No sources found")

    clip = {
        'inputSources': srcList,
        'jobIdentity': { 'skyTileId': skyTile },
    }

    clip = runStage(apCluster.SourceClusteringStage,
        """#<?cfg paf policy?>
        inputKeys: {
        }
        outputKeys: {
        }
        """, clip)

    clip = runStage(apCluster.SourceClusterAttributesStage,
        """#<?cfg paf policy?>
        inputKeys: {
        }
        outputKeys: {
        }
        """, clip)

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

def test():
    sourceAssocProcess(root=".", outRoot=".")

def main():
    lsstSimMain(sourceAssocProcess, "source", "skyTile")

if __name__ == "__main__":
    main()
