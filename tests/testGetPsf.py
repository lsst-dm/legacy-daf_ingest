#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008-2014 LSST Corporation.
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

import unittest

import lsst.utils.tests as utilsTests

from lsst.datarel.utils import getPsf
import lsst.daf.persistence as dafPersist

class GetPsfTest(unittest.TestCase):
    """
    Test for retrieving a PSF from an exposure.
    """

    def testGetPsf(self):
        """Test the getPSF function."""

        dataId = dict(visit=85408556, filter="r", raft="2,3", sensor="1,1")
        butler = dafPersist.Butler("tests/data")
        self.assertEqual(butler.datasetExists("calexp", dataId), True)
        # For now, we don't have a test calexp with a Psf, so just test that
        # we try to retrieve one and don't throw an exception.
        psf = getPsf(butler, "calexp", dataId, strict=False, warn=False)
        self.assertEqual(psf, None)

#####

def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(GetPsfTest)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
