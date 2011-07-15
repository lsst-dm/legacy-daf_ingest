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
import sys
import os
import unittest

import numpy

import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.datarel as lsstDatarel
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
from lsst.pex.harness.simpleStageTester import SimpleStageTester

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
class VigCorrTestCase(unittest.TestCase):
    """A test case for the VigCorr stage using Simple Stage Tester"""

    def testStage(self):
        """Pipeline test case
        """
        policyFile = pexPolicy.DefaultPolicyFile("datarel", "policy/VigCorrStageDictionary.paf")
        policy = pexPolicy.Policy.createPolicy(policyFile)
        policy.set("inputKeys.exposure", "inExposure")
        policy.set("inputKeys.vigCorrImage", "vigCorrImage")
        policy.set("outputKeys.corrExposure", "corrExposure")
        
        for doCorrect in (False, True):
            for doMultiply in (False, True):
                afwDim = afwGeom.Extent2I(5, 8)
                numpyShape = (afwDim[1], afwDim[0])
                corrFac = 0.1
                
                inMI = afwImage.MaskedImageF(afwDim)
                inImageArr = inMI.getImage().getArray()
                dataArr = numpy.arange(1, (afwDim[0] * afwDim[1]) + 1)
                dataArr.shape = numpyShape
                inImageArr[:] = dataArr
                inExposure = afwImage.ExposureF(inMI)
                vigCorrImage = afwImage.ImageF(afwDim)
                vigCorrArr = vigCorrImage.getArray()
                vigCorrArr[:] = dataArr * corrFac
                
                if doCorrect:
                    if doMultiply:
                        expectedCorrData = dataArr * (dataArr * corrFac)
                    else:
                        expectedCorrData = dataArr / (dataArr * corrFac)
                else:
                    expectedCorrData = dataArr
                
                policy.set("parameters.doCorrect", doCorrect)
                policy.set("parameters.doMultiply", doMultiply)
                
                vigCorrStage = lsstDatarel.VigCorrStage(policy)
                vigCorrSst = SimpleStageTester(vigCorrStage)

                clipboard = pexClipboard.Clipboard()
                clipboard.put(policy.get("inputKeys.exposure"), inExposure)
                clipboard.put(policy.get("inputKeys.vigCorrImage"), vigCorrImage)
        
                vigCorrOut = vigCorrSst.runWorker(clipboard)
                corrExposure = vigCorrOut.get(policy.get("outputKeys.corrExposure"))
                corrData = corrExposure.getMaskedImage().getImage().getArray()
                
                if not numpy.allclose(corrData, expectedCorrData):
                    self.fail("Image data does not match")

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    suites += unittest.makeSuite(VigCorrTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)
