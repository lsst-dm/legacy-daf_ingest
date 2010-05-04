#!/usr/bin/env python

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
def addExtensions(defpol, outfile = "defects.fits"):
    defects = cgUtils.makeDefects(defpol)
    hdulist = prepareFits()
    for key in defects.keys():
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
        hdr.update('serial', key.getSerial())
        hdr.update('name', key.getName())
        hdulist.append(tbhdu)
    hdulist.writeto(outfile)
def fitsToDefectDict(hdulist):
    defects = {}
    for hdu in hdulist[1:]:
        id = cameraGeom.Id(hdu.header['serial'], hdu.header['name'])
        print id
        data = hdu.data
        defectList = []
        for i in range(len(data)):
            bbox = afwImage.BBox(afwImage.PointI(int(data[i]['x0']),\
                int(data[i]['y0'])), int(data[i]['width']),\
                int(data[i]['height']))
            defectList.append(measAlg.Defect(bbox))
        defects[id] = defectList
    return defects

if __name__ == "__main__":
    defpol = pexPolicy.Policy(sys.argv[1])
    #addExtensions(defpol)
    hdulist = pf.open(sys.argv[2])
    t1 = time.time()
    defects = cgUtils.makeDefects(defpol)
    print time.time() - t1
    t1 = time.time()
    defects = fitsToDefectDict(hdulist)
    print time.time() - t1
