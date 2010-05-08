#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.daf.persistence as dafPersist
import lsst.ap.cluster as apCluster

from lsst.obs.lsstSim import LsstSimMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester


def sourceAssocProcess(root=None, outRoot=None, inButler=None, outButler=None, **keys):

    if inButler is None:
        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=root))
        inButler = bf.create()
    if outButler is None:
        obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outRoot))
        outButler = obf.create()
    srcList = []
    raftSensorList = inButler.queryMetadata("raw", "raft", ("visit", "raft", "sensor"),
                                            skyTile=keys['skyTile'])
    for visit, raft, sensor in raftSensorList:
        srcs = butler.get("src", visit=visit, raft=raft, sensor=sensor)
        srcList.append(srcs)

    clip = {
        'sources': srcList,
        'jobIdentity': { 'skyTileId': keys['skyTile'] },
    }

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
        }
        outputKeys: {
        }
        """))
    sc = SimpleStageTester(apCluster.SourceClusteringStage(pol))

    pol = pexPolicy.Policy.createPolicy(pexPolicy.PolicyString(
        """#<?cfg paf policy?>
        inputKeys: {
        }
        outputKeys: {
        }
        """))
    sca = SimpleStageTester(apCluster.SourceClusterAttributesStage(pol))

    clip = sc.runWorker(clip)
    clip = sca.runWorker(clip)
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

def run():
    sourceAssocProcess(root=".", outRoot=".", skyTile=1)

if __name__ == "__main__":
    run()
