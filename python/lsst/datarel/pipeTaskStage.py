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
import importlib

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
        taskModule = self.policy.get("parameters.taskModule")
        taskClass = self.policy.get("parameters.taskClass")
        self.taskClass = getattr(
                importlib.import_module(taskModule), taskClass)

        # Tokens for substitution into the above command template
        self.tokens = {}

        # get the input and output directories
        inputLocation = persistence.LogicalLocation("%(input)")
        self.tokens['input'] = inputLocation.locString()

        outputLocation = persistence.LogicalLocation("%(output)")
        self.tokens['output'] = outputLocation.locString()


    def process(self, clipboard):
        """
        Execute a pipe_task.
        """
        self.log.log(Log.INFO, "PipeTaskStage - process call")

        ds = clipboard.get("inputDatasets")

        ds = ds[0]

        # get the clipboard entries for raft, sensor and visit.  
        self.tokens['raft'] = str(ds.ids["raft"])
        self.tokens['sensor'] =  str(ds.ids["sensor"])
        self.tokens['visit'] = str(ds.ids["visit"])


        # execute the task, configuring it through the argument parser
        # to ensure reproducibility from the command line
        
        cmd = self.cmdTemplate % self.tokens
        self.log.log(Log.INFO, "PipeTaskStage - cmd = %s" % (cmd,))
        namespace = self.parser.parse_args(
                config=self.taskClass.ConfigClass(),
                args=cmd.split(), log=self.log)
        task = self.taskClass(namespace.config)
        for sensorRef in namespace.dataRefList:
            sensorRef.put(namespace.config, "pipe_config")
            try:
                task.run(sensorRef)
            except Exception, e:
                self.log.log(task.log.FATAL, "Failed on dataId=%s: %s" %
                        (sensorRef.dataId, e))
                raise
            sensorRef.put(task.getFullMetadata(), "pipe_metadata")

        self.log.log(Log.INFO, "PipeTaskStage - done.")

    
class PipeTaskStage(harnessStage.Stage):
    parallelClass = PipeTaskStageParallel
