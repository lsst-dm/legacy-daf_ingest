#!/usr/bin/env python

from lsst.datarel import cfhtMain, cfhtSetup, runStage

import lsst.ip.pipeline as ipPipe

def ccdAssemblyProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry, None,
            inButler, outButler)

    expList = []
    for amp in (0, 1):
        expList.append(inButler.get("postISR", amp=amp, **keys))

    clip = {
        'exposureList': expList
    }

    clip = runStage(ipPipe.IsrCcdAssemblyStage,
        """#<?cfg paf policy?>
        outputKeys: {
            assembledCcdExposure: isrExposure
        }
        """, clip)
    clip = runStage(ipPipe.IsrCcdDefectStage,
        """#<?cfg paf policy?>
        inputKeys: {
            ccdExposure: isrExposure
        }
        """, clip)

    outButler.put(clip['defectMaskedCcdExposure'], "postISRCCD", **keys)

def test():
    root="."
    ccdAssemblyProcess(root=root, outRoot=".", visit=788965, ccd=0)

def main():
    cfhtMain(ccdAssemblyProcess, "postISRCCD", "ccd")

if __name__ == "__main__":
    main()
