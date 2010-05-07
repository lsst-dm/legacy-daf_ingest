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

    srcList = []
    ccdList = inButler.queryMetadata("raw", "ccd", ("visit", "ccd"),
                                     skyTile=keys['skyTile'])
    for visit, ccd in ccdList:
        if ccd == 6: # hack (only ccd for which we have sources) 
            srcs = inButler.get("src", visit=visit, ccd=ccd)
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
    sourceAssocProcess(root=".", outRoot=".", skyTile=100477)

if __name__ == "__main__":
    run()

