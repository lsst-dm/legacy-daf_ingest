#!/usr/bin/env python

from __future__ import with_statement

import unittest
import lsst.utils.tests as utilsTests

import glob
import os
import subprocess
import sys

from ISR_ImSim import isrProcess
from CcdAssembly_ImSim import ccdAssemblyProcess
from CrSplit_ImSim import crSplitProcess
from ImgChar_ImSim import imgCharProcess
from SFM_ImSim import sfmProcess

import eups
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper

def process(inButler, tmpButler, outButler, visit, raft, sensor, force=False):
    print >>sys.stderr, "****** Processing visit %d raft %s sensor %s" % \
            (visit, raft, sensor)
    if tmpButler is not None:
        if force or not outButler.datasetExists("calexp",
                visit=visit, raft=raft, sensor=sensor):
            for snap in inButler.queryMetadata("raw", "snap"):
                for channel in inButler.queryMetadata("raw", "channel"):
                    isrProcess(inButler=inButler, outButler=tmpButler,
                            visit=visit, snap=snap,
                            raft=raft, sensor=sensor, channel=channel)
                ccdAssemblyProcess(inButler=tmpButler, outButler=tmpButler,
                        visit=visit, snap=snap, raft=raft, sensor=sensor)
            crSplitProcess(inButler=tmpButler, outButler=tmpButler,
                    visit=visit, raft=raft, sensor=sensor)
            imgCharProcess(inButler=tmpButler, outButler=outButler,
                    visit=visit, raft=raft, sensor=sensor)
        sfmProcess(inButler=outButler, outButler=outButler,
                visit=visit, raft=raft, sensor=sensor)
        return

    if force or not outButler.datasetExists("calexp",
            visit=visit, raft=raft, sensor=sensor):
        if force or not outButler.datasetExists("visitim",
                visit=visit, raft=raft, sensor=sensor):
            for snap in inButler.queryMetadata("raw", "snap"):
                if force or not outButler.datasetExists("postISRCCD",
                        visit=visit, snap=snap, raft=raft, sensor=sensor):
                    for channel in inButler.queryMetadata("raw", "channel"):
                        if force or not outButler.datasetExists("postISR",
                                visit=visit, snap=snap,
                                raft=raft, sensor=sensor, channel=channel):
                            isrProcess(inButler=inButler, outButler=outButler,
                                    visit=visit, snap=snap,
                                    raft=raft, sensor=sensor, channel=channel)
                    ccdAssemblyProcess(inButler=outButler, outButler=outButler,
                            visit=visit, snap=snap, raft=raft, sensor=sensor)
            crSplitProcess(inButler=outButler, outButler=outButler,
                    visit=visit, raft=raft, sensor=sensor)
        imgCharProcess(inButler=outButler, outButler=outButler,
                visit=visit, raft=raft, sensor=sensor)
    sfmProcess(inButler=outButler, outButler=outButler,
            visit=visit, raft=raft, sensor=sensor)

def compare(fname):
    stat = subprocess.call(["cmp", fname,
        os.path.join(eups.productDir("afwdata"), "ImSim", fname)])
    return stat == 0

class EndToEndTestCase(unittest.TestCase):
    """Testing end to end (through SFM) PT1 processing"""

    def testEndToEnd(self):
        """Test ISR, CcdAssembly, CrSplit, ImgChar, SFM pipelines"""

        self.assert_(eups.Eups().isSetup("obs_lsstSim"))
        self.assert_(eups.Eups().isSetup("astrometry_net_data"))
        self.assert_(eups.Eups().findSetupVersion("astrometry_net_data")[0].startswith("imsim_"))

        inputRoot = os.path.join(eups.productDir("afwdata"), "ImSim")
        outputRoot = "tests"
        registryPath = os.path.join(inputRoot, "registry.sqlite3")

        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=inputRoot))
        inButler = bf.create()
        obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outputRoot,
            registry=registryPath))
        outButler = obf.create()
        if not os.path.exists("/tmp/DC3"):
            os.mkdir("/tmp/DC3")
        tmpDir = os.path.join("/tmp/DC3", str(os.getpid()))
        if os.path.exists(tmpDir):
            print >>sys.stderr, "WARNING: %s exists, reusing" % (tmpDir,)
        else:
            os.mkdir(tmpDir)
        tbf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=tmpDir,
            registry=registryPath))
        tmpButler = tbf.create()

        tmpSdqaAmp = os.path.join(tmpDir, "sdqaAmp/")
        sdqaAmp = os.path.join(outputRoot, "sdqaAmp")
        if not os.path.exists(sdqaAmp):
            os.mkdir(sdqaAmp)

        tmpSdqaCcd = os.path.join(tmpDir, "sdqaCcd/")
        sdqaCcd = os.path.join(outputRoot, "sdqaCcd")
        if not os.path.exists(sdqaCcd):
            os.mkdir(sdqaCcd)

        process(inButler, tmpButler, outButler, 85408556, "2,3", "1,1", True)
        if os.path.exists(tmpSdqaAmp):
            subprocess.call(["rsync", "-a", tmpSdqaAmp, sdqaAmp])
        if os.path.exists(tmpSdqaCcd):
            subprocess.call(["rsync", "-a", tmpSdqaCcd, sdqaCcd])
        subprocess.call(["rm", "-r", tmpDir])

        os.chdir("tests")

        # SDQA boost persistence saves some uninitialized values that will be
        # set later upon database ingest, so those cannot be compared in this
        # naive way.

        for fname in (
                "calexp/v85408556-fr/R23/S11.fits",
                "icSrc/v85408556-fr/R23/S11.boost",
                "psf/v85408556-fr/R23/S11.boost", 
                # "sdqaCcd/v85408556-fr/s0/R23/S11.boost",
                # "sdqaCcd/v85408556-fr/s1/R23/S11.boost",
                "src/v85408556-fr/R23/S11.boost"):
            self.assert_(compare(fname), "%s is different" % (fname,))

        # for fname in glob.glob("sdqaAmp/v85408556-fr/s*/R23/S11/C*.boost"):
        #     self.assert_(compare(fname), "%s is different" % (fname,))

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(EndToEndTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
