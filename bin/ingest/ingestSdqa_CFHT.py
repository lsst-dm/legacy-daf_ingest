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

import os
import sys

import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper

from lsst.datarel.csvFileWriter import CsvFileWriter

class CsvGenerator(object):
    def __init__(self, root, registry=None):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        bf = dafPersist.ButlerFactory(
                mapper=CfhtMapper(root=root, registry=registry))
        self.butler = bf.create()

        self.ampFile = CsvFileWriter("sdqa_Rating_ForScienceAmpExposure.csv")
        self.ccdFile = CsvFileWriter("sdqa_Rating_ForScienceCcdExposure.csv")
        self.rawToSnapFile = CsvFileWriter("Raw_Amp_To_Snap_Ccd_Exposure.csv")
        self.snapToSciFile = \
                CsvFileWriter("Snap_Ccd_To_Science_Ccd_Exposure.csv")

    def csvAll(self):
        for visit, ccd in self.butler.queryMetadata("raw", "ccd",
                ("visit", "ccd")):
            if self.butler.datasetExists("sdqaCcd", visit=visit, snap=0,
                    ccd=ccd):
                self.toCsv(visit, ccd)

    def toCsv(self, visit, ccd):
        sciCcdExposureId = (long(visit) << 6) + ccd

        snapCcdExposureId = sciCcdExposureId
        self.snapToSciFile.write(snapCcdExposureId, 0, sciCcdExposureId)

        prv = self.butler.get("sdqaCcd", visit=visit, ccd=ccd)
        for r in prv.getSdqaRatings():
            self.ccdFile.write(r.getName(), snapCcdExposureId,
                    r.getValue(), r.getErr())

        for amp in xrange(2):
            rawAmpExposureId = (snapCcdExposureId << 1) + amp
            self.rawToSnapFile.write(rawAmpExposureId, amp, snapCcdExposureId)

            prv = self.butler.get("sdqaAmp", visit=visit, ccd=ccd, amp=amp)
            for r in prv.getSdqaRatings():
                self.ampFile.write(r.getName(), rawAmpExposureId,
                        r.getValue(), r.getErr())
        print "Processed visit %d ccd %d" % (visit, ccd)

def main():
    registry = None
    if len(sys.argv) >= 3:
        root = sys.argv[1]
        registry = sys.argv[2]
    elif len(sys.argv) >= 2:
        root = sys.argv[1]
    else:
        root = "/lsst/DC3/data/datarel/CFHTLS/D2_aggregate"
    c = CsvGenerator(root, registry)
    c.csvAll()

if __name__ == '__main__':
    main()
