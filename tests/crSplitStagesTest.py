#!/usr/bin/env python
"""
Run with:
   python crSplitStageTest.py
or
   python
   >>> import crSplitStageTest
   >>> crSplitStageTest.run()
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
import lsst.afw.math as afwMath

from lsst.pex.harness.simpleStageTester import SimpleStageTester

import lsst.meas.pipeline as measPipe
import lsst.meas.utils.cosmicRays as cosmicRays

try:
    type(display)
except NameError:
    display = False
    displayAll = not False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class CrSplitStageTestCase(unittest.TestCase):
    """A test case for CrSplitStage.py"""

    def fakeCRSplitExposures(self, exposure, nCR=25):
        """Fake a split exposure"""
        self.removeCRsSilently(exposure)

        mi = exposure.getMaskedImage()
        mi = mi.Factory(mi, True)

        image = exposure.getMaskedImage().getImage()
        seed = int(afwMath.makeStatistics(image, afwMath.MAX).getValue())

        CRs = image.Factory(image.getDimensions())
        cosmicRays.addCosmicRays(CRs, nCR=nCR, seed=seed)
        image += CRs

        seed = int(afwMath.makeStatistics(image, afwMath.MEAN).getValue())
        CRs.set(0)
        cosmicRays.addCosmicRays(CRs, nCR=nCR, seed=seed)
        image = mi.getImage()
        image += CRs

        exposure2 = afwImage.makeExposure(mi, exposure.getWcs())
        exposure2.setMetadata(exposure.getMetadata())

        return [exposure, exposure2]
        
    def removeCRsSilently(self, exposure):
        """Remove CRs without trace"""
        mask = exposure.getMaskedImage().getMask()
        mask = mask.Factory(mask, True) # save initial mask

        policyFile = pexPolicy.DefaultPolicyFile("datarel", 
                                                 "crSplitStages_policy.paf", "tests")
        policy = pexPolicy.Policy.createPolicy(policyFile)
        #
        # Modify the policy;  delete the CRs even if the policy wants to keep them,
        # and set policy.exposure from policy.exposure0
        #
        policy.set("CrRejectStage.parameters.keepCRs", False)
        for io in ["input", "output"]:
            policy.set("CrRejectStage.%sKeys.exposure" % io,
                       policy.get("CrRejectStage.%sKeys.exposure0" % io))

        stage = ipPipe.CrRejectStage(policy.get("CrRejectStage"))
        tester = SimpleStageTester(stage)
        
        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("CrRejectStage.inputKeys.exposure"), exposure)

        tester.runWorker(clipboard)
        omask = exposure.getMaskedImage().getMask()
        omask <<= mask

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"), "CFHT", "D4", "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        exposure = afwImage.ExposureF(filename, 0,bbox)

        self.exposures = self.fakeCRSplitExposures(exposure)

    def tearDown(self):
        del self.exposures

    def testPipeline(self):
        policyFile = pexPolicy.DefaultPolicyFile("datarel", 
                                           "crSplitStages_policy.paf", "tests")
        policy = pexPolicy.Policy.createPolicy(policyFile)

        #-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

        nexp = len(self.exposures)

        tester = SimpleStageTester()
        #
        # Setup the first two stages, each of which needs to be run twice (once per Exposure)
        #
        for stageClass in [measPipe.BackgroundEstimationStage, ipPipe.CrRejectStage]:
            if stageClass == ipPipe.CrRejectStage:
                continue
            
            for i in range(nexp):
                stageName = stageClass.__name__
                stagePolicy = pexPolicy.Policy(policy.get(stageName), True)
                #
                # Patch the policy to process this exposure
                #
                for io in ["input", "output"]:
                    stagePolicy.set("%sKeys.exposure" % (io),
                                    policy.get("%s.%sKeys.exposure%d" % (stageName, io, i)))
                    
                stage = stageClass(stagePolicy)
                tester.addStage(stage)
            
        stage = ipPipe.SimpleDiffImStage(policy.get("SimpleDiffImStage"))
        tester.addStage(stage)

        stage = measPipe.SourceDetectionStage(policy.get("SourceDetectionStage"))
        tester.addStage(stage)

        stage = ipPipe.CrSplitCombineStage(policy.get("CrSplitCombineStage"))
        tester.addStage(stage)
        #
        # Load the clipboard
        #
        clipboard = pexClipboard.Clipboard()

        for i in range(nexp):
            if displayAll and display:
                ds9.mtv(self.exposures[i], frame=i, title="Input%d" % i)

            clipboard.put(policy.get("BackgroundEstimationStage.inputKeys.exposure%d" % i), self.exposures[i])
        #
        # Do the work
        #
        outClipboard = tester.runWorker(clipboard)
        #
        # See if we got it right
        #
        for i in range(nexp):
            outPolicy = policy.get("BackgroundEstimationStage.outputKeys")
            assert(outClipboard.contains(outPolicy.get("exposure%d" % i)))

            outPolicy = policy.get("CrRejectStage.outputKeys")

            if displayAll and display:
                ds9.mtv(outClipboard.get(outPolicy.get("exposure%d" % i)),
                        frame=nexp + i, title="no CR %d" % i)
                
            self.assertTrue(outClipboard.contains(outPolicy.get("exposure%d" % i)))

        outPolicy = policy.get("SourceDetectionStage.outputKeys")
        
        if displayAll and display:
            ds9.mtv(outClipboard.get(outPolicy.get("backgroundSubtractedExposure")),
                    frame=2*nexp, title="diffim")

        outPolicy = policy.get("CrSplitCombineStage.outputKeys")
        
        if display:
            ds9.mtv(outClipboard.get(outPolicy.get("combinedExposure")),
                    frame=2*nexp + 1, title="combined")
        

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(CrSplitStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

