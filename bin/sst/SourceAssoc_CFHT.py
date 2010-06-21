#!/usr/bin/env python

import os
import lsst.pex.policy as pexPolicy
import lsst.daf.persistence as dafPersist
import lsst.ap.cluster as apCluster

from lsst.obs.cfht import CfhtMapper
from lsst.pex.harness.simpleStageTester import SimpleStageTester


def sourceAssocProcess(root=None, outRoot=None, inButler=None, outButler=None, **keys):

    if inButler is None:
        bf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=root))
        inButler = bf.create()
    if outButler is None:
        obf = dafPersist.ButlerFactory(mapper=CfhtMapper(root=outRoot))
        outButler = obf.create()

    if 'skyTile' in keys:
        skyTiles = [(keys['skyTile'],)]
    else:
        skyTiles = inButler.queryMetadata("raw", "skytile")
        if len(skyTiles) == 0:
            raise RuntimeError('No sky-tiles found')

    srcList = []
    while len(skyTiles) > 0 and len(srcList) == 0:
        skyTile = skyTiles.pop()
        ccdList = inButler.queryMetadata("raw", "ccd", ("visit", "ccd"), skyTile=skyTile)
        for visit, ccd in ccdList:
            if inButler.datasetExists("src", visit=visit, ccd=ccd):
                srcs = inButler.get("src", visit=visit, ccd=ccd)
                srcList.append(srcs)
    if len(srcList) == 0:
        raise RuntimeError("No sources found")
    keys['skyTile'] = skyTile

    clip = {
        'sources': srcList,
        'jobIdentity': { 'skyTileId': skyTile },
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
    sourceAssocProcess(root=".", outRoot=".")

if __name__ == "__main__":
    run()

