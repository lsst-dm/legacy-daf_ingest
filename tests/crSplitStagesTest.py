#!/usr/bin/env python
"""
Run with:
   python crRejectStageTest.py
or
   python
   >>> import crRejectStageTest
   >>> crRejectStageTest.run()
"""

import sys, os, math
from math import *

import pdb
import unittest

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.afw.image as afwImage
import lsst.afw.display.ds9 as ds9

from lsst.pex.harness.simpleStageTester import SimpleStageTester

import lsst.meas.pipeline as measPipe

try:
    type(display)
except NameError:
    display = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class CrRejectStageTestCase(unittest.TestCase):
    """A test case for CrRejectStage.py"""

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"), "CFHT", "D4", "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        self.exposure = afwImage.ExposureF(filename, 0,bbox)        

    def tearDown(self):
        del self.exposure        

    def testPipeline(self):
        file = pexPolicy.DefaultPolicyFile("datarel", 
                                           "crSplitStages_policy.paf", "tests")
        policy = pexPolicy.Policy.createPolicy(file)

        #-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

        stage = measPipe.BackgroundEstimationStage(policy.get("backgroundEstimationStage"))
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("backgroundEstimationStage.inputKeys.exposure"), self.exposure)

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outClipboard = tester.runWorker(clipboard)

        outPolicy = policy.get("backgroundEstimationStage.outputKeys")
        assert(outClipboard.contains(outPolicy.get("backgroundSubtractedExposure")))
        assert(outClipboard.contains(outPolicy.get("background")))

        if display:
            ds9.mtv(outClipboard.get(outPolicy.get("backgroundSubtractedExposure")),
                    frame=1, title="Subtracted")

        #-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

        stage = ipPipe.CrRejectStage(policy.get("crRejectStage"))
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("crRejectStage.inputKeys.exposure"), self.exposure)

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outClipboard = tester.runWorker(clipboard)

        outPolicy = policy.get("crRejectStage.outputKeys")
        self.assertTrue(outClipboard.contains(outPolicy.get("exposure")))
        self.assertEqual(outClipboard.get("nCR"), 25)

        if display:
            ds9.mtv(outClipboard.get(outPolicy.get("exposure")), frame=1, title="CR removed")

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(CrRejectStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

