#!/usr/bin/env python

import os
import sys

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage

from lsst.datarel.csvFileWriter import CsvFileWriter

filterMap = ["u.MP9301", "g.MP9401", "r.MP9601", "i.MP9701", "z.MP9801",
        "i2.MP9702"]

class CsvGenerator(object):
    def __init__(self, root, registry=None):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        self.mapper = CfhtMapper(root=root, registry=registry)
        bf = dafPersist.ButlerFactory(mapper=self.mapper)
        self.butler = bf.create()

        self.expFile = CsvFileWriter("Raw_Amp_Exposure.csv")
        self.mdFile = CsvFileWriter("Raw_Amp_Exposure_Metadata.csv")
        self.rToSFile = CsvFileWriter("Raw_Amp_To_Science_Ccd_Exposure.csv")

    def csvAll(self):
        for visit, ccd in self.butler.queryMetadata("raw", "ccd",
                ("visit", "ccd")):
            if self.butler.datasetExists("raw", visit=visit, ccd=ccd, amp=0):
                self.toCsv(visit, ccd)

    def getFullMetadata(self, datasetType, **keys):
        filename = self.mapper.map(datasetType, keys).getLocations()[0]
        return afwImage.readMetadata(filename)

    def toCsv(self, visit, ccd):
        sciCcdExposureId = (long(visit) << 6) + ccd

        for amp in (0, 1):
            rawAmpExposureId = (sciCcdExposureId << 1) + amp

            self.rToSFile.write(rawAmpExposureId, sciCcdExposureId, 0, amp)

            md = self.getFullMetadata("raw", visit=visit, ccd=ccd, amp=amp)

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
                    visit, 0, 0, ccd, amp,
                    filterMap.index(md.get('FILTER').strip()),
                    md.get('RA_DEG'), md.get('DEC_DEG'),
                    md.get('EQUINOX'),
                    md.get('RADECSYS'), # note wrong name
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
                    obsStart, obsMidpoint.toString(), expTime,
                    md.get('AIRMASS'), md.get('DARKTIME'),
                    None) # ZENITH is missing
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

        print "Processed visit %d ccd %d" % (visit, ccd)

def main():
    registry = None
    if len(sys.argv) >= 3:
        root = sys.argv[1]
        registry = sys.argv[2]
    elif len(sys.argv) >= 2:
        root = sys.argv[1]
    else:
        root = "/lsst/DC3/data/obstest/CFHTLS"
    c = CsvGenerator(root, registry)
    c.csvAll()

if __name__ == '__main__':
    main()
