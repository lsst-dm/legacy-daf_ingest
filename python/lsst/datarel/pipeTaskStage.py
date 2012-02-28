#!/usr/bin/env python
import re
import time
import lsst.pex.harness.stage as harnessStage
import lsst.daf.persistence as persistence
import string

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import subprocess

from lsst.pipe.base import ArgumentParser
import lsst.pipe.tasks.processCcdLsstSim import ProcessCcdLsstSimTask as TaskClass

class PipeTaskStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage grabs data from the clip board and uses that data as
        arguments for the executing an external script.

    """
    def setup(self):
        self.log = Log(self.log, "PipeTaskStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("datarel",
                "PipeTaskStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath(), True)
        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

        self.parser = ArgumentParser()
        self.cmdTemplate = self.policy.get("parameters.cmdTemplate")

    def process(self, clipboard):
        """
        Execute a pipe_task.
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


        # execute the task, configuring it through the argument parser
        # to ensure reproducibility from the command line
        
        tokens = dict(input=input, output=output, visit=visit, raft=raft,
                sensor=sensor)
        cmd = self.cmdTemplate % tokens
        self.log.log(Log.INFO, "PipeTaskStage - cmd = %s" % (cmd,))
        namespace = self.parser.parse_args(config=TaskClass.ConfigClass(),
                args=cmd.split(), log=self.log)
        task = TaskClass(namespace.config)
        for sensorRef in namespace.dataRefList:
            try:
                task.run(sensorRef)
            except Exception, e:
                self.log.log(task.log.FATAL, "Failed on dataId=%s: %s" %
                        (sensorRef.dataId, e))
                raise

        self.log.log(Log.INFO, "PipeTaskStage - done.")

    
    def sanitizeInput(self, input_str):
        valid_chars = "-/_.()+, %s%s" % (string.ascii_letters, string.digits)

        return ''.join(c for c in input_str if c in valid_chars)

class PipeTaskStage(harnessStage.Stage):
    parallelClass = PipeTaskStageParallel
