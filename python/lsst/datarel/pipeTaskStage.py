#!/usr/bin/env python
import os
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

        self.name = self.policy.get("parameters.taskName")
        self.parser = ArgumentParser(self.name)
        self.cmdTemplate = self.policy.get("parameters.cmdTemplate")
        taskModule = self.policy.get("parameters.taskModule")
        taskClass = self.policy.get("parameters.taskClass")
        self.dataIdNames = self.policy.getArray("parameters.dataIdNames")
        self.taskClass = getattr(
                importlib.import_module(taskModule), taskClass)

        # Tokens for substitution into the above command template
        self.tokens = {}
        for name in os.environ:
            self.tokens[name] = os.environ[name]

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

        # get the clipboard entries for the dataId
        for name in self.dataIdNames:
            self.tokens[name] = str(ds.ids[name])

        # execute the task

        commandLine = self.cmdTemplate % self.tokens
        self.log.log(Log.INFO, "PipeTaskStage - cmd = %s" % (commandLine,))
        self.taskClass.parseAndRun(commandLine.split(), log=self.log)
        self.log.log(Log.INFO, "PipeTaskStage - done.")

    
class PipeTaskStage(harnessStage.Stage):
    parallelClass = PipeTaskStageParallel
