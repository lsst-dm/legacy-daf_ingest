#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":

    print "starting...\n"

    eventBrokerHost = "lsst8.ncsa.uiuc.edu" 

    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, "triggerSFMEvent")

    root = PropertySet()

    root.set("inputPathName", "871034p_1_MI")
    # root.set("inputPathName", "small_MI")
    # root.set("inputPathName", "med")
    root.set("sourcePathName", "source")
    root.set("detectionPathName", "detection")
    root.set("ampExposureId", 1)

    externalEventTransmitter.publish(root)

