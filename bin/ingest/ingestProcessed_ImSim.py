#!/usr/bin/env python

import os
import sys

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
from lsst.obs.lsstSim import LsstSimMapper
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage
import lsst.meas.algorithms as measAlg

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

        self.expFile = CsvFileWriter("Science_Ccd_Exposure.csv")
        self.mdFile = CsvFileWriter("Science_Ccd_Exposure_Metadata.csv")

    def csvAll(self):
        for visit, raft, sensor in self.butler.queryMetadata("raw", "sensor",
                ("visit", "raft", "sensor")):
            if self.butler.datasetExists("calexp", visit=visit, raft=raft,
                    sensor=sensor):
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

        md = self.getFullMetadata("calexp",
                visit=visit, raft=raft, sensor=sensor)
        width = md.get('NAXIS1')
        height = md.get('NAXIS2')
        wcs = afwImage.makeWcs(md)
        llc = wcs.pixelToSky(0, 0).toIcrs()
        ulc = wcs.pixelToSky(0, height - 1).toIcrs()
        urc = wcs.pixelToSky(width - 1, height - 1).toIcrs()
        lrc = wcs.pixelToSky(width - 1, 0).toIcrs()
        psf = self.butler.get("psf", visit=visit, raft=raft, sensor=sensor)
        attr = measAlg.PsfAttributes(psf, width // 2, height // 2)
        fwhm = attr.computeGaussianWidth()
        obsStart = dafBase.DateTime(md.get('MJD-OBS'), dafBase.DateTime.MJD,
                dafBase.DateTime.UTC)
        self.expFile.write(sciCcdExposureId, visit, raftNum, ccdNum,
                filterMap.index(md.get('FILTER').strip()),
                md.get('RA_DEG'), md.get('DEC_DEG'),
                md.get('EQUINOX'), md.get('RADESYS'),
                md.get('CTYPE1'), md.get('CTYPE2'),
                md.get('CRPIX1'), md.get('CRPIX2'),
                md.get('CRVAL1'), md.get('CRVAL2'),
                md.get('CD1_1'), md.get('CD1_2'),
                md.get('CD2_1'), md.get('CD2_2'),
                llc.getRa(afwCoord.DEGREES), llc.getDec(afwCoord.DEGREES),
                ulc.getRa(afwCoord.DEGREES), ulc.getDec(afwCoord.DEGREES),
                urc.getRa(afwCoord.DEGREES), urc.getDec(afwCoord.DEGREES),
                lrc.getRa(afwCoord.DEGREES), lrc.getDec(afwCoord.DEGREES),
                obsStart.get(dafBase.DateTime.MJD, dafBase.DateTime.TAI),
                obsStart,
                md.get('TIME-MID'), md.get('EXPTIME'),
                1, 1, 1,
                md.get('RDNOISE'), md.get('SATURATE'), md.get('GAINEFF'),
                md.get('FLUXMAG0'), md.get('FLUXMAG0ERR'),
                fwhm)
        for name in md.paramNames():
            if md.typeOf(name) == md.TYPE_Int:
                self.mdFile.write(sciCcdExposureId, 1, name,
                        md.getInt(name), None, None)
            elif md.typeOf(name) == md.TYPE_Double:
                self.mdFile.write(sciCcdExposureId, 1, name,
                        None, md.getDouble(name), None)
            else:
                self.mdFile.write(sciCcdExposureId, 1, name,
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
        root = "/lsst/DC3/data/datarel/ImSim/ktl20100701"
    c = CsvGenerator(root, registry)
    c.csvAll()

if __name__ == '__main__':
    main()
