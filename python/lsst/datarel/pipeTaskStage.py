#!/usr/bin/env python
import re
import time
import lsst.pex.harness.stage as harnessStage
import lsst.daf.persistence as persistence
import string

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import subprocess

class PipeTaskStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage grabs data from the clip board and uses that data as
        arguments for the executing an external script.

    """
    def setup(self):
        self.log = Log(self.log, "PipeTaskStage - parallel")


        if self.policy is None:
            self.policy = pexPolicy.Policy()

    def process(self, clipboard):
        """
        Clear the clipboard of everything except inputKeys, which are remapped
        to outputKeys.
        """
        self.log.log(Log.INFO, "PipeTaskStage - process call")

        ds = clipboard.get("inputDatasets")

        ds = ds[0]

        # get the input and output directories, and get the clipboard
        # entries for raft, sensor and visit.  
        # sanitize them all since we're going to execute a shell
        inputLocation = persistence.LogicalLocation("%(input)")
        input = self.sanitizeInput(inputLocation.locString())

        outputLocation = persistence.LogicalLocation("%(output)")
        output = self.sanitizeInput(outputLocation.locString())


        raft = self.sanitizeInput(str(ds.ids["raft"]))
        sensor =  self.sanitizeInput(str(ds.ids["sensor"]))
        visit = self.sanitizeInput(str(ds.ids["visit"]))


        # build and execute the shell command
        cmdTemplate = "processCcdLsstSim.py lsstSim %s --output %s --id visit=%s raft=%s sensor=%s"
        cmd = cmdTemplate % (input, output, visit, raft, sensor)

        subprocess.call(cmd.split())

        self.log.log(Log.INFO, "PipeTaskStage - done.")

    
    def sanitizeInput(self, input_str):
        valid_chars = "-/_.()+, %s%s" % (string.ascii_letters, string.digits)

        return ''.join(c for c in input_str if c in valid_chars)

class PipeTaskStage(harnessStage.Stage):
    parallelClass = PipeTaskStageParallel
