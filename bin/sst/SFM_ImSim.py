#!/usr/bin/env python

from lsst.datarel import lsstSimMain, lsstSimSetup, runStage

import lsst.meas.pipeline as measPipe

def sfmProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = lsstSimSetup(root, outRoot, registry, None,
            inButler, outButler)

    clip = {
        'scienceExposure': inButler.get("calexp", **keys),
        'psf': inButler.get("psf", **keys)
    }

    clip = runStage(measPipe.SourceDetectionStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: scienceExposure
            psf: psf
        }
        outputKeys: {
            positiveDetection: positiveFootprintSet
        }
        backgroundPolicy: {
            algorithm: NONE
        }
        """, clip)

    clip = runStage(measPipe.SourceMeasurementStage,
        """#<?cfg paf policy?>
        inputKeys: {
            exposure: scienceExposure
            psf: psf
            positiveDetection: positiveFootprintSet
        }
        outputKeys: {
            sources: sourceSet
        }
        """, clip)

    clip = runStage(measPipe.ComputeSourceSkyCoordsStage,
        """#<?cfg paf policy?>
        inputKeys: {
            sources: sourceSet
            exposure: scienceExposure
        }
        """, clip)

    outButler.put(clip['sourceSet_persistable'], "src", **keys)

    fields = ("XAstrom", "XAstromErr", "YAstrom", "YAstromErr", 
            "PsfFlux", "ApFlux", "Ixx", "IxxErr", "Iyy",
            "IyyErr", "Ixy", "IxyErr")
    csv = open("sources-v%(visit)d-R%(raft)s-S%(sensor)s.csv" % keys, "w")
    print >>csv, "FlagForDetection," + ",".join(fields)
    for s in clip['sourceSet']:
        line = "%d" % (s.getFlagForDetection(),)
        for f in fields:
            func = getattr(s, "get" + f)
            line += ",%g" % (func(),)
        print >>csv, line
    csv.close()

def test():
    sfmProcess(root=".", outRoot=".", visit=85751839, raft="2,3", sensor="1,1")

def main():
    lsstSimMain(sfmProcess, "src", "ccd")

if __name__ == "__main__":
    main()
