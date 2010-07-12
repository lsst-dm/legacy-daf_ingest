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

"""Code and constants to control which stages the ImgChar_*.py functions run"""
#
# Stages to run
#
DETECT =                0x1
MEASURE = DETECT        << 1
PSF = MEASURE           << 1
WCS = PSF               << 1
WCS_VERIFY = WCS        << 1
PHOTO_CAL = WCS_VERIFY  << 1
ALL_STAGES = (PHOTO_CAL << 1) - 1

def setPrerequisites(stages):
    """Given a desired set of stages, add in any pre-requisites that must also be run"""
    
    if not stages:
        return ALL_STAGES

    if stages & (WCS_VERIFY | PHOTO_CAL):
        stages |= WCS
    if stages & WCS:
        stages |= MEASURE
    if stages & MEASURE:
        stages |= DETECT

    return stages
