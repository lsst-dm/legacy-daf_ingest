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

import lsst.afw.geom as afwGeom

def getDataset(butler, dataset, dataId, strict, warn):
    """Get a dataset from a repository with an optional exception or warning if not found

    @param[in] butler: data butler
    @param[in] dataset: name of desired dataset
    @param[in] dataId: data ID dict
    @param[in] strict: if True then raise RuntimeError if dataset not found
    @param[in] warn: if True and strict False then print a warning to stderr if dataset not found
    
    @raise RuntimeError if dataset not found and strict true
    """
    try:
        ds = butler.get(dataset, dataId=dataId, immediate=True)
    except:
        ds = None
    if ds == None:
        msg = '{} : Failed to retrieve {} dataset'.format(dataId, dataset)
        if strict:
            raise RuntimeError(msg)
        elif warn:
            print >>sys.stderr, '*** Skipping ' + msg
    return ds

def getPsf(butler, dataset, dataId, strict, warn):
    """Get the PSF from a repository without reading (very much of) the exposure
    
    @param[in] butler: data butler
    @param[in] dataset: name of desired dataset
    @param[in] dataId: data ID dict of exposure containing desired PSF
    @param[in] strict: if True then raise RuntimeError if psf not found
    @param[in] warn: if True and strict False then print a warning to stderr if psf not found
    
    @raise RuntimeError if exposure not found (regardless of strict)
    @raise RuntimeError if exposure has no PSF and strict true
    """
    # there is not yet a way to read just the PSF, so read a 1x1 subregion of the exposure
    tinyBBox = afwGeom.Box2I(afwGeom.Point2I(0,0), afwGeom.Extent2I(1,1))
    tinyExposure = butler.get(dataset + "_sub", dataId=dataId, bbox=tinyBBox, imageOrigin="LOCAL", immediate=True)
    psf = tinyExposure.getPsf()
    if psf is None:
        msg = '%s : %s exposure had no PSF' % (dataId, dataset)
        psf = None
        if strict:
            raise RuntimeError(msg)
        elif warn:
            print >>sys.stderr, '*** Skipping ' + msg
    return psf
