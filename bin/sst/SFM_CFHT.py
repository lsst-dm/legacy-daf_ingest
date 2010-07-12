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


from lsst.datarel import cfhtMain, cfhtSetup, runStage

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

#    fields = ("XAstrom", "XAstromErr", "YAstrom", "YAstromErr", 
#            "PsfFlux", "ApFlux", "Ixx", "IxxErr", "Iyy",
#            "IyyErr", "Ixy", "IxyErr")
#    csv = open("sources-v%(visit)d-c%(ccd)d.csv" % keys, "w")
#    print >>csv, "FlagForDetection," + ",".join(fields)
#    for s in clip['sourceSet']:
#        line = "%d" % (s.getFlagForDetection(),)
#        for f in fields:
#            func = getattr(s, "get" + f)
#            line += ",%g" % (func(),)
#        print >>csv, line
#    csv.close()

def test():
    sfmProcess(root=".", outRoot=".", visit=788965, ccd=6)

def main():
    cfhtMain(sfmProcess, "src", "ccd")

if __name__ == "__main__":
    main()
