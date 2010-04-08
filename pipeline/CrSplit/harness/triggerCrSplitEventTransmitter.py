#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":

    print "starting...\n"

    eventBrokerHost = "lsst8.ncsa.uiuc.edu" 

    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, "triggerCrSplitEvent")

    root = PropertySet()

    root.set("outputPathName", "CrSplitExposure")
    root.set("inputPathName0", "imsim_85751839_R23_S11_C04_E000")
    root.set("inputPathName1", "imsim_85751839_R23_S11_C04_E001")

    # root.set("inputPathName", "small_MI")
    # root.set("inputPathName", "med")




    # root.setInt("visitId", 1)
    # root.setDouble("FOVRa", 273.48066298343)

    externalEventTransmitter.publish(root)

