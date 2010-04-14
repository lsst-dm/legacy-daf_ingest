#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.pex.policy as pexPolicy
import os
import eups

import lsst.ctrl.events as events
import time

if __name__ == "__main__":

    print "starting...\n"

    eventBrokerHost = "lsst8.ncsa.uiuc.edu"
    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, "triggerImgCharEvent")

    # sym link the 'input' to directory containing sample calibrated images
    # set the name of one calibrated image 
    root.set("inputPathName", "871034p_1_MI")

    externalEventTransmitter.publish(root)

