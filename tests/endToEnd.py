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


from __future__ import with_statement

import unittest
import lsst.utils.tests as utilsTests

import glob
import os
import re
import subprocess
import sys

from ISR_ImSim import isrProcess
from CcdAssembly_ImSim import ccdAssemblyProcess
from CrSplit_ImSim import crSplitProcess
from ImgChar_ImSim import imgCharProcess
from SFM_ImSim import sfmProcess

import eups
import lsst.afw.detection as afwDet
import lsst.afw.math as afwMath
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper

def calexpCompare(o1, o2):
    w1 = o1.getWcs().getFitsMetadata().toString()
    w2 = o2.getWcs().getFitsMetadata().toString()
    if w1 != w2:
        return "calexp WCS:\n%s\n%s" % (w1, w2)
    m1 = o1.getMetadata().toString()
    m2 = o2.getMetadata().toString()
    if m1 != m2:
        return "calexp metadata:\n%s\n%s" % (m1, m2)
    c1 = o1.getCalib()
    c2 = o2.getCalib()
    if c1.getMidTime().nsecs() != c2.getMidTime().nsecs():
        return "calexp calib midTime: %s %s" % (
                c1.getMidTime().toString(), c2.getMidTime().toString())
    if c1.getExptime() != c2.getExptime():
        return "calexp calib exptime: %s %s" % (
                c1.getExptime(), c2.getExptime())
    if c1.getFluxMag0() != c2.getFluxMag0():
        return "calexp calib exptime: %s %s" % (
                str(c1.getFluxMag0()), str(c2.getFluxMag0()))
    if o1.getHeight() != o2.getHeight():
        return "calexp height: %s %s" % (o1.getHeight(), o2.getHeight())
    if o1.getWidth() != o2.getWidth():
        return "calexp width: %s %s" % (o1.getWidth(), o2.getWidth())

    im = o1.getMaskedImage().getImage()
    im -= o2.getMaskedImage().getImage()
    st = afwMath.makeStatistics(im, afwMath.MAX | afwMath.MIN)
    if st.getValue(afwMath.MAX) > 1.0e-8:
        return "calexp img max diff = %g" % (st.getValue(afwMath.MAX),)
    if st.getValue(afwMath.MIN) < -1.0e-8:
        return "calexp img min diff = %g" % (st.getValue(afwMath.MIN),)

    var = o1.getMaskedImage().getVariance()
    var -= o2.getMaskedImage().getVariance()
    st = afwMath.makeStatistics(var, afwMath.MAX | afwMath.MIN)
    if st.getValue(afwMath.MAX) > 1.0e-8:
        return "calexp var max diff = %g" % (st.getValue(afwMath.MAX),)
    if st.getValue(afwMath.MIN) < -1.0e-8:
        return "calexp var min diff = %g" % (st.getValue(afwMath.MIN),)

    mask = o1.getMaskedImage().getMask()
    mask ^= o2.getMaskedImage().getMask()
    st = afwMath.makeStatistics(mask, afwMath.SUM)
    if st.getValue(afwMath.SUM) != 0:
        return "calexp mask sum = %d" % (st.getValue(afwMath.SUM),)

    return None

def cmpSrc(t, s1, s2, rtol=1e-10, atol=1e-8):
    for getField in dir(s1):
        if not getField.startswith("get"):
            continue

        nullField = getField[3:]
        nullField = re.sub(r'.[A-Z]',
                lambda m: m.group(0)[0] + '_' + m.group(0)[1], nullField)
        nullField = nullField.upper()
        if hasattr(afwDet, nullField):
            num = getattr(afwDet, nullField)
            if s1.isNull(num) != s2.isNull(num):
                return "%s %s null: %s %s" % (t, field,
                        str(s1.isNull(num)), str(s2.isNull(num)))
            if s1.isNull(num):
                continue

        v1 = getattr(s1, getField)()
        v2 = getattr(s2, getField)()
        if str(v1) == "nan" and str(v2) == "nan":
            continue
        if abs(v1 - v2) <= atol and (v1 == 0 or abs(v1 - v2) / v1 <= rtol):
            continue
        return "%s %s: %g %g" % (t, getField, v1, v2)

    return None

def srcCompare(o1, o2, t="src"):
    src1 = o1.getSources()
    src2 = o2.getSources()
    if len(src1) != len(src2):
        return "%s length: %d %d" % (t, len(src1), len(src2))
    for s1, s2 in zip(src1, src2):
        msg = cmpSrc(t, s1, s2)
        if msg is not None:
            return msg

def icSrcCompare(o1, o2):
    return srcCompare(o1, o2, t="icSrc")

def sdqaCompare(t, o1, o2):
    r1 = o1.getSdqaRatings()
    r2 = o2.getSdqaRatings()
    if len(r1) != len(r2):
        return "%s lengths: %d, %d" % (t, len(r1), len(r2))
    for i in xrange(len(r1)):
        if r1[i].getName() != r2[i].getName():
            return "%s names: %s, %s" % (t, r1[i].getName(), r2[i].getName())
        if r1[i].getValue() != r2[i].getValue():
            return "%s %s values: %g, %g" % (t, r1[i].getName(),
                    r1[i].getValue(), r2[i].getValue())
        if r1[i].getErr() != r2[i].getErr():
            return "%s %s errors: %g, %g" % (t, r1[i].getName(),
                    r1[i].getErr(), r2[i].getErr())
        if r1[i].getRatingScope() != r2[i].getRatingScope():
            return "%s %s scope: %d, %d" % (t, r1[i].getName(),
                    r1[i].getRatingScope(), r2[i].getRatingScope())
    return None

def sdqaAmpCompare(o1, o2):
    return sdqaCompare("sdqaAmp", o1, o2)

def sdqaCcdCompare(o1, o2):
    return sdqaCompare("sdqaCcd", o1, o2)

def compare(butler, cmpButler, datasetType, **keys):
    o1 = butler.get(datasetType, **keys)
    o2 = cmpButler.get(datasetType, **keys)
    return eval(datasetType + "Compare(o1, o2)")

class EndToEndTestCase(unittest.TestCase):
    """Testing end to end (through SFM) PT1 processing"""

    def testEndToEnd(self):
        """Test ISR, CcdAssembly, CrSplit, ImgChar, SFM pipelines"""

        self.assert_(eups.Eups().isSetup("obs_lsstSim"))
        self.assert_(eups.Eups().isSetup("astrometry_net_data"))
        self.assert_(eups.Eups().findSetupVersion("astrometry_net_data")[0].startswith("imsim_"))

        inputRoot = os.path.join(eups.productDir("afwdata"), "ImSim")
        if os.path.exists("endToEnd.py"):
            outputRoot = "."
        else:
            outputRoot = "tests"

        registryPath = os.path.join(inputRoot, "registry.sqlite3")

        bf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=inputRoot))
        inButler = bf.create()
        obf = dafPersist.ButlerFactory(mapper=LsstSimMapper(root=outputRoot,
            registry=registryPath))
        outButler = obf.create()

        stat = subprocess.call(["runImSim.py", "-T", "--force",
            "-i", inputRoot, "-o", outputRoot,
            "-v", "85408556", "-r", "2,3", "-s", "1,1"])
        self.assertEqual(stat, 0, "Error while running end to end test")

        fname = "psf/v85408556-fr/R23/S11.boost"
        stat = subprocess.call(["cmp",
            os.path.join(outputRoot, fname), os.path.join(inputRoot, fname)])
        self.assertEqual(stat, 0, "psf differs")

        for datasetType in ("icSrc", "src", "calexp"):
            msg = compare(outButler, inButler, datasetType,
                    visit=85408556, raft="2,3", sensor="1,1")
            self.assert_(msg is None, msg)

        for snap in (0, 1):
            msg = compare(outButler, inButler, "sdqaCcd",
                visit=85408556, snap=snap, raft="2,3", sensor="1,1")
            self.assert_(msg is None, msg)
            for channel in inButler.queryMetadata("raw", "channel"):
                msg = compare(outButler, inButler, "sdqaAmp",
                    visit=85408556, snap=snap, raft="2,3", sensor="1,1",
                    channel=channel)
                self.assert_(msg is None, msg)

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