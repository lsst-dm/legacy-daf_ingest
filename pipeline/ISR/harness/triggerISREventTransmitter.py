#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *
import os
import eups

import lsst.ctrl.events as events
import time

if __name__ == "__main__":

    print "starting...\n"

    eventBrokerHost = "lsst8.ncsa.uiuc.edu"

    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, "triggerISREvent")

    root = PropertySet()
  
    filename = os.path.join(eups.productDir("afwdata"), "CFHT", "D4", "cal-53535-i-797722_1_img.fits")
    root.set("inputPathName", filename)
    # should create: /lsst/DC3/stacks/gcc433/04jun/Linux64/afwdata/svn1397/CFHT/D4/cal-53535-i-797722_1_img.fits
    print "inputPathname: %s" % (filename)

    externalEventTransmitter.publish(root)

