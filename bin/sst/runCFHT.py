#!/usr/bin/env python

# setup datarel
# setup obs_cfht
# setup astrometry_net_data cfhtlsDeep

from optparse import OptionParser
import os
import subprocess
import sys
import traceback

from ISR_CFHT import isrProcess
from CcdAssembly_CFHT import ccdAssemblyProcess
from CrSplit_CFHT import crSplitProcess
from ImgChar_CFHT import imgCharProcess
from SFM_CFHT import sfmProcess
import lsst.daf.base as dafBase

import lsst.daf.persistence as dafPersist
from lsst.obs.cfht import CfhtMapper

def process(inButler, tmpButler, outButler, visit, ccd, force=False):
    print >>sys.stderr, "****** Processing visit %d ccd %d: %s" % \
            (visit, ccd, dafBase.DateTime.now().toString())
    if outButler.datasetExists("src", visit=visit, ccd=ccd):
        return
    if tmpButler is not None:
        if force or not outButler.datasetExists("calexp",
                visit=visit, ccd=ccd):
            for amp in (0, 1):
                isrProcess(inButler=inButler, outButler=tmpButler,
                        visit=visit, ccd=ccd, amp=amp)
            ccdAssemblyProcess(inButler=tmpButler, outButler=tmpButler,
                    visit=visit, ccd=ccd)
            crSplitProcess(inButler=tmpButler, outButler=tmpButler,
                    visit=visit, ccd=ccd)
            imgCharProcess(inButler=tmpButler, outButler=outButler,
                    visit=visit, ccd=ccd)
        sfmProcess(inButler=outButler, outButler=outButler,
                visit=visit, ccd=ccd)
        return

    if force or not outButler.datasetExists("calexp", visit=visit, ccd=ccd):
        if force or not outButler.datasetExists("visitim",
                visit=visit, ccd=ccd):
            if force or not outButler.datasetExists("postISRCCD",
                    visit=visit, ccd=ccd):
                for amp in (0, 1):
                    if force or not outButler.datasetExists("postISR",
                            visit=visit, ccd=ccd, amp=amp):
                        isrProcess(inButler=inButler, outButler=outButler,
                                visit=visit, ccd=ccd, amp=amp)
                ccdAssemblyProcess(inButler=outButler,
                        outButler=outButler, visit=visit, ccd=ccd)
            crSplitProcess(inButler=outButler, outButler=outButler,
                    visit=visit, ccd=ccd)
        imgCharProcess(inButler=outButler, outButler=outButler,
                visit=visit, ccd=ccd)
    sfmProcess(inButler=outButler, outButler=outButler, visit=visit, ccd=ccd)

def main():
    parser = OptionParser()
    parser.add_option("-i", "--input", dest="root",
            default="/lsst/DC3/data/obstest/CFHTLS", help="input root")
    parser.add_option("-o", "--output", dest="outRoot", default=".",
            help="output root")
    parser.add_option("-f", "--force", action="store_true",
            default=False,
            help="execute even if output dataset exists")
    parser.add_option("-C", "--calibRoot", dest="calibRoot",
            help="calibration root")
    parser.add_option("-R", "--registry", help="registry")
    parser.add_option("-T", "--tmp", action="store_true",
            default=False, help="use /tmp for intermediates")
    parser.add_option("-v", "--visit", action="append", type="int",
            help="visit numbers (can be repeated)")
    parser.add_option("-c", "--ccd", action="append", type="int",
            help="ccd number (can be repeated)")
    (options, args) = parser.parse_args()

    if options.registry is None:
        if os.path.exists(os.path.join(options.root, "registry.sqlite3")):
            options.registry = os.path.join(options.root, "registry.sqlite3")
    if options.registry is None:
        if os.path.exists("/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"):
            options.registry = "/lsst/DC3/data/obstest/CFHTLS/registry.sqlite3"
    if options.calibRoot is None:
        if os.path.exists("/lsst/DC3/data/obstest/CFHTLS/calib"):
            options.calibRoot = "/lsst/DC3/data/obstest/CFHTLS/calib"

    bf = dafPersist.ButlerFactory(mapper=CfhtMapper(
        root=options.root, calibRoot=options.calibRoot,
        registry=options.registry))
    inButler = bf.create()
    obf = dafPersist.ButlerFactory(mapper=CfhtMapper(
        root=options.outRoot, calibRoot=options.calibRoot,
        registry=options.registry))
    outButler = obf.create()
    tmpButler = None
    if options.tmp:
        if not os.path.exists("/tmp/DC3"):
            os.mkdir("/tmp/DC3")
        tmpDir = os.path.join("/tmp/DC3", str(os.getpid()))
        if os.path.exists(tmpDir):
            print >>sys.stderr, "WARNING: %s exists, reusing" % (tmpDir,)
        else:
            os.mkdir(tmpDir)
        tbf = dafPersist.ButlerFactory(mapper=CfhtMapper(
            root=tmpDir, calibRoot=options.calibRoot,
            registry=options.registry))
        tmpButler = tbf.create()

        tmpSdqaAmp = os.path.join(tmpDir, "sdqaAmp/")
        sdqaAmp = os.path.join(options.outRoot, "sdqaAmp")
        if not os.path.exists(sdqaAmp):
            os.mkdir(sdqaAmp)

        tmpSdqaCcd = os.path.join(tmpDir, "sdqaCcd/")
        sdqaCcd = os.path.join(options.outRoot, "sdqaCcd")
        if not os.path.exists(sdqaCcd):
            os.mkdir(sdqaCcd)
 
    if options.visit is None:
        print >>sys.stderr, "Running over all input visits"
        options.visit = inButler.queryMetadata("raw", "visit")
    elif not hasattr(options.visit, "__iter__"):
        options.visit = [options.visit]
    if options.ccd is None:
        print >>sys.stderr, "Running over all CCDs"
        options.ccd = inButler.queryMetadata("raw", "ccd")
    elif not hasattr(options.ccd, "__iter__"):
        options.ccd = [options.ccd] 

    for visit in options.visit:
        for ccd in options.ccd:
            try:
                process(inButler, tmpButler, outButler,
                        visit, ccd, options.force)
                if options.tmp:
                    if os.path.exists(tmpSdqaAmp):
                        subprocess.call(["rsync", "-a", tmpSdqaAmp, sdqaAmp])
                    if os.path.exists(tmpSdqaCcd):
                        subprocess.call(["rsync", "-a", tmpSdqaCcd, sdqaCcd])
                    subprocess.call(["rm", "-r", tmpDir])
            except Exception, e:
                traceback.print_exc()
                print >>sys.stderr, "Continuing..."

if __name__ == "__main__":
    main()
