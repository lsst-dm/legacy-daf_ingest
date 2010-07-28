#!/usr/bin/env python

import os
import sys

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper
import lsst.afw.coord as afwCoord
import lsst.afw.image as afwImage
import lsst.meas.algorithms as measAlg

from lsst.datarel.csvFileWriter import CsvFileWriter

filterMap = ["u", "g", "r", "i", "z", "i2"]

class CsvGenerator(object):
    def __init__(self, root, registry=None):
        if registry is None:
            registry = os.path.join(root, "registry.sqlite3")
        self.mapper = CfhtMapper(root=root, registry=registry)
        bf = dafPersist.ButlerFactory(mapper=self.mapper)
        self.butler = bf.create()

        self.expFile = CsvFileWriter("Science_Ccd_Exposure.csv")
        self.mdFile = CsvFileWriter("Science_Ccd_Exposure_Metadata.csv")

    def csvAll(self):
        for visit, ccd in self.butler.queryMetadata("raw", "ccd",
                ("visit", "ccd")):
            if self.butler.datasetExists("calexp", visit=visit, ccd=ccd):
                self.toCsv(visit, ccd)

    def getFullMetadata(self, datasetType, **keys):
        filename = self.mapper.map(datasetType, keys).getLocations()[0]
        return afwImage.readMetadata(filename)

    def toCsv(self, visit, ccd):
        sciCcdExposureId = (long(visit) << 6) + ccd

        md = self.getFullMetadata("calexp", visit=visit, ccd=ccd)
        width = md.get('NAXIS1')
        height = md.get('NAXIS2')
        wcs = afwImage.makeWcs(md.deepCopy())
        llc = wcs.pixelToSky(0, 0).toIcrs()
        ulc = wcs.pixelToSky(0, height - 1).toIcrs()
        urc = wcs.pixelToSky(width - 1, height - 1).toIcrs()
        lrc = wcs.pixelToSky(width - 1, 0).toIcrs()
        psf = self.butler.get("psf", visit=visit, ccd=ccd)
        attr = measAlg.PsfAttributes(psf, width // 2, height // 2)
        fwhm = attr.computeGaussianWidth()
        obsStart = dafBase.DateTime(md.get('MJD-OBS'), dafBase.DateTime.MJD,
                dafBase.DateTime.UTC)
        self.expFile.write(sciCcdExposureId, visit, 0, ccd,
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
