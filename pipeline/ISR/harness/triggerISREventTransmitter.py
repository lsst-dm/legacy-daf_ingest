#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom.utils as cameraGeomUtils
import lsst.pex.policy as pexPolicy
import math
import os
import eups

import lsst.ctrl.events as events
import time

if __name__ == "__main__":

    print "starting...\n"

    eventBrokerHost = "lsst8.ncsa.uiuc.edu"
    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, "triggerISREvent")

    externalEventTransmitter.publish()

