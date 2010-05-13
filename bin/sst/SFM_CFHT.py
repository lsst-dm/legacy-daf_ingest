#!/usr/bin/env python

from utils import cfhtMain, cfhtSetup, runStage

import lsst.meas.pipeline as measPipe

def sfmProcess(root=None, outRoot=None, registry=None,
        inButler=None, outButler=None, **keys):
    inButler, outButler = cfhtSetup(root, outRoot, registry,
            None, inButler, outButler)

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
    csv = open("sources-v%(visit)d-c%(ccd)d.csv" % keys, "w")
    print >>csv, "FlagForDetection," + ",".join(fields)
    for s in clip['sourceSet']:
        line = "%d" % (s.getFlagForDetection(),)
        for f in fields:
            func = getattr(s, "get" + f)
            line += ",%f" % (func(),)
        print >>csv, line
    csv.close()

def test():
    sfmProcess(root=".", outRoot=".", visit=788965, ccd=6)

def main():
    cfhtMain(sfmProcess, "src", "ccd")

if __name__ == "__main__":
    main()
