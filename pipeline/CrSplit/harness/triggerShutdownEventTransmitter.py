#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    shutdownTopic = "shutdownCrSplit"
    eventBrokerHost = "lsst8.ncsa.uiuc.edu"

    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, shutdownTopic )

    root = PropertySet()
    # Shutdown at level 1 : stop immediately by killing process (ugly)
    # Shutdown at level 2 : exit in a clean manner (Pipeline and Slices) at a synchronization point 
    # Shutdown at level 3 : exit in a clean manner (Pipeline and Slices) at the end of a Stage
    # Shutdown at level 4 : exit in a clean manner (Pipeline and Slices) at the end of a Visit
    root.setInt("level", 1)

    externalEventTransmitter.publish(root)

