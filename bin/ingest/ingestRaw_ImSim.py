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

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage

from lsst.datarel.csvFileWriter import CsvFileWriter

rafts = [       "0,1", "0,2", "0,3",
         "1,0", "1,1", "1,2", "1,3", "1,4",
         "2,0", "2,1", "2,2", "2,3", "2,4",
         "3,0", "3,1", "3,2", "3,3", "3,4",
                "4,1", "4,2", "4,3"]

filterMap = ["u", "g", "r", "i", "z", "y"]

class CsvGenerator(object):
    def __init__(self, root, registry=None):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        self.mapper = LsstSimMapper(root=root, registry=registry)
        bf = dafPersist.ButlerFactory(mapper=self.mapper)
        self.butler = bf.create()

        self.expFile = CsvFileWriter("Raw_Amp_Exposure.csv")
        self.mdFile = CsvFileWriter("Raw_Amp_Exposure_Metadata.csv")
        self.rToSFile = CsvFileWriter("Raw_Amp_To_Science_Ccd_Exposure.csv")

    def csvAll(self):
        for visit, raft, sensor in self.butler.queryMetadata("raw", "sensor",
                ("visit", "raft", "sensor")):
            if self.butler.datasetExists("raw", visit=visit, snap=0,
                    raft=raft, sensor=sensor, channel="0,0"):
                self.toCsv(visit, raft, sensor)

    def getFullMetadata(self, datasetType, **keys):
        filename = self.mapper.map(datasetType, keys).getLocations()[0]
        return afwImage.readMetadata(filename)

    def toCsv(self, visit, raft, sensor):
        r1, comma, r2 = raft
        s1, comma, s2 = sensor
        raftNum = rafts.index(raft)
        raftId = int(r1) * 5 + int(r2)
        ccdNum = int(s1) * 3 + int(s2)
        sciCcdExposureId = (long(visit) << 9) + raftId * 10 + ccdNum

        for snap in xrange(2):
            rawCcdExposureId = (sciCcdExposureId << 1) + snap

            for channelY in xrange(2):
                for channelX in xrange(8):
                    channel = "%d,%d" % (channelY, channelX)
                    channelNum = (channelY << 3) + channelX
                    rawAmpExposureId = (rawCcdExposureId << 4) + channelNum

                    try:
                        md = self.getFullMetadata("raw",
                                visit=visit, snap=snap,
                                raft=raft, sensor=sensor, channel=channel)
                    except:
                        print ("*** Unable to read metadata for " + \
                                "visit %d snap %d " + \
                                "raft %s sensor %s channel %s") % \
                                (visit, snap, raft, sensor, channel)
                        continue

                    self.rToSFile.write(rawAmpExposureId, sciCcdExposureId,
                            snap, channelNum)

                    width = md.get('NAXIS1')
                    height = md.get('NAXIS2')
                    wcs = afwImage.makeWcs(md.deepCopy())
                    llc = wcs.pixelToSky(0, 0).toIcrs()
                    ulc = wcs.pixelToSky(0, height - 1).toIcrs()
                    urc = wcs.pixelToSky(width - 1, height - 1).toIcrs()
                    lrc = wcs.pixelToSky(width - 1, 0).toIcrs()
                    obsStart = dafBase.DateTime(md.get('MJD-OBS'),
                            dafBase.DateTime.MJD, dafBase.DateTime.UTC)
                    expTime = md.get('EXPTIME')
                    obsMidpoint = dafBase.DateTime(obsStart.nsecs() +
                            long(expTime * 1000000000L / 2))
                    self.expFile.write(rawAmpExposureId,
                            visit, snap, raftNum, ccdNum, channelNum,
                            filterMap.index(md.get('FILTER').strip()),
                            md.get('RA_DEG'), md.get('DEC_DEG'),
                            md.get('EQUINOX'), md.get('RADESYS'),
                            md.get('CTYPE1'), md.get('CTYPE2'),
                            md.get('CRPIX1'), md.get('CRPIX2'),
                            md.get('CRVAL1'), md.get('CRVAL2'),
                            md.get('CD1_1'), md.get('CD1_2'),
                            md.get('CD2_1'), md.get('CD2_2'),
                            llc.getRa(afwCoord.DEGREES),
                            llc.getDec(afwCoord.DEGREES),
                            ulc.getRa(afwCoord.DEGREES),
                            ulc.getDec(afwCoord.DEGREES),
                            urc.getRa(afwCoord.DEGREES),
                            urc.getDec(afwCoord.DEGREES),
                            lrc.getRa(afwCoord.DEGREES),
                            lrc.getDec(afwCoord.DEGREES),
                            obsStart.get(dafBase.DateTime.MJD,
                                dafBase.DateTime.TAI),
                            obsStart,
                            obsMidpoint.get(dafBase.DateTime.MJD,
                                dafBase.DateTime.TAI),
                            expTime,
                            md.get('AIRMASS'), md.get('DARKTIME'),
                            md.get('ZENITH'))
                    for name in md.paramNames():
                        if md.typeOf(name) == md.TYPE_Int:
                            self.mdFile.write(rawAmpExposureId, 1, name,
                                    md.getInt(name), None, None)
                        elif md.typeOf(name) == md.TYPE_Double:
                            self.mdFile.write(rawAmpExposureId, 1, name,
                                    None, md.getDouble(name), None)
                        else:
                            self.mdFile.write(rawAmpExposureId, 1, name,
                                    None, None, str(md.get(name)))

        print "Processed visit %d raft %s sensor %s" % (visit, raft, sensor)

def main():
    registry = None
    if len(sys.argv) >= 3:
        root = sys.argv[1]
        registry = sys.argv[2]
    elif len(sys.argv) >= 2:
        root = sys.argv[1]
    else:
        root = "/lsst/DC3/data/obstest/ImSim"
    c = CsvGenerator(root, registry)
    c.csvAll()

if __name__ == '__main__':
    main()
