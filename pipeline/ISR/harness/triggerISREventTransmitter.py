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

    obsDir = eups.productDir('obs_lsstSim') 
    print 'obs_lsstSim:',obsDir
    policyFile = pexPolicy.DefaultPolicyFile("afw",
                "CameraGeomDictionary.paf", "policy")
    defPolicy = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath(), True)
    geomPolicy =\
        pexPolicy.Policy.createPolicy(os.path.join(obsDir, "description", "Full_STA_geom.paf"), True)
    geomPolicy.mergeDefaults(defPolicy.getDictionary())
    cameraPolicy = geomPolicy



    if self.policy is None:
            self.policy = pexPolicy.Policy()
    self.policy.mergeDefaults(defPolicy)
    cameraPolicy = clipboard.get(self.policy.getString("inputKeys.cameraPolicy"))
    camera = cameraGeomUtils.makeCamera(cameraPolicy)

    root = PropertySet()
    root.set("cameraInfo", camera)

    externalEventTransmitter.publish(root)

