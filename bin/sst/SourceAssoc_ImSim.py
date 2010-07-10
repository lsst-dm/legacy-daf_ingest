#!/usr/bin/env python

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
        'sources': srcList,
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
