#!/usr/bin/env python

import os
import sys

import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper

from lsst.datarel.csvFileWriter import CsvFileWriter

rafts = [       "0,1", "0,2", "0,3",
         "1,0", "1,1", "1,2", "1,3", "1,4",
         "2,0", "2,1", "2,2", "2,3", "2,4",
         "3,0", "3,1", "3,2", "3,3", "3,4",
                "4,1", "4,2", "4,3"]

class CsvGenerator(object):
    def __init__(self, root, registry=None):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        bf = dafPersist.ButlerFactory(
                mapper=LsstSimMapper(root=root, registry=registry))
        self.butler = bf.create()

        self.ampFile = CsvFileWriter("sdqa_Rating_ForScienceAmpExposure.csv")
        self.ccdFile = CsvFileWriter("sdqa_Rating_ForScienceCcdExposure.csv")
        self.rawToSnapFile = CsvFileWriter("Raw_Amp_To_Snap_Ccd_Exposure.csv")
        self.snapToSciFile = \
                CsvFileWriter("Snap_Ccd_To_Science_Ccd_Exposure.csv")

    def csvAll(self):
        for visit, raft, sensor in self.butler.queryMetadata("raw", "sensor",
                ("visit", "raft", "sensor")):
            if self.butler.datasetExists("sdqaCcd", visit=visit, snap=0,
                    raft=raft, sensor=sensor):
                self.toCsv(visit, raft, sensor)

    def toCsv(self, visit, raft, sensor):
        r1, comma, r2 = raft
        s1, comma, s2 = sensor
        raftNum = rafts.index(raft)
        raftId = int(r1) * 5 + int(r2)
        ccdNum = int(s1) * 3 + int(s2)
        sciCcdExposureId = (long(visit) << 9) + raftId * 10 + ccdNum

        for snap in xrange(2):
            # PT1 obs_lsstSim generates incorrect exposureIds without snaps.
            # Replace them with the correct ones.
            snapCcdExposureId = (sciCcdExposureId << 1) + snap
            self.snapToSciFile.write(snapCcdExposureId, snap, sciCcdExposureId)

            prv = self.butler.get("sdqaCcd",
                    visit=visit, snap=snap, raft=raft, sensor=sensor)
            for r in prv.getSdqaRatings():
                self.ccdFile.write(r.getName(), snapCcdExposureId,
                        r.getValue(), r.getErr())

            for channelY in xrange(2):
                for channelX in xrange(8):
                    channel = "%d,%d" % (channelY, channelX)
                    channelNum = (channelY << 3) + channelX
                    rawAmpExposureId = (snapCcdExposureId << 4) + channelNum
                    self.rawToSnapFile.write(rawAmpExposureId, channelNum,
                            snapCcdExposureId)

                    prv = self.butler.get("sdqaAmp", visit=visit, snap=snap,
                            raft=raft, sensor=sensor, channel=channel)
                    for r in prv.getSdqaRatings():
                        self.ampFile.write(r.getName(), rawAmpExposureId,
                                r.getValue(), r.getErr())
        print "Processed visit %d raft %s sensor %s" % (visit, raft, sensor)

def main():
    registry = None
    if len(sys.argv) >= 3:
        root = sys.argv[1]
        registry = sys.argv[2]
    elif len(sys.argv) >= 2:
        root = sys.argv[1]
    else:
        root = "/lsst/DC3/data/datarel/ImSim/ktl20100701"
    c = CsvGenerator(root, registry)
    c.csvAll()

if __name__ == '__main__':
    main()
