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


import time
import pyfits as pf
import lsst.afw.cameraGeom.utils as cgUtils
import lsst.meas.algorithms as measAlg
import lsst.afw.image as afwImage
import lsst.afw.cameraGeom as cameraGeom
import lsst.pex.policy as pexPolicy
import numpy as np
import sys

def prepareFits():
    hdu = pf.PrimaryHDU()
    hdulist = pf.HDUList([hdu])
    return hdulist
def makeFitsDefectsFromPaf(defpol, outfileroot = "defects"):
    defects = cgUtils.makeDefects(defpol)
    for key in defects.keys():
        outfile = outfileroot+str(key.getSerial())+".fits"
        hdulist = prepareFits()
        x0 = []
        y0 = []
        width = []
        height = []
        for defect in defects[key]:
            bbox = defect.getBBox()
            x0.append(bbox.getX0())
            y0.append(bbox.getY0())
            width.append(bbox.getWidth())
            height.append(bbox.getHeight())
        x0 = np.asarray(x0)
        y0 = np.asarray(y0)
        width = np.asarray(width)
        height = np.asarray(height)
        col1 = pf.Column(name="x0", format="I", array=x0)
        col2 = pf.Column(name="y0", format="I", array=y0)
        col3 = pf.Column(name="width", format="I", array=width)
        col4 = pf.Column(name="height", format="I", array=height)
        cols = pf.ColDefs([col1,col2,col3,col4])
        tbhdu=pf.new_table(cols)
        hdr = tbhdu.header
        hdr.update('SERIAL', key.getSerial())
        hdr.update('NAME', key.getName())
        hdr.update('CDATE', time.asctime(time.localtime()))

        hdulist.append(tbhdu)
        hdulist.writeto(outfile, clobber=True)

if __name__ == "__main__":
    defpol = pexPolicy.Policy(sys.argv[1])
    makeFitsDefectsFromPaf(defpol)
