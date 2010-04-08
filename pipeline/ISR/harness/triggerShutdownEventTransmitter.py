#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "shutdownISR")

    root = PropertySet()
  
    root.set("visitid", "fov391")

    externalEventTransmitter.publish(root)

