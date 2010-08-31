#!/usr/bin/env python
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy

class FixupStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage is used for cleaning up the clipboard in the PT1 unified
        pipeline.  It takes its inputKeys from the clipboard and then clears
        the clipboard completely.  It puts the objects that were in the
        inputKeys back on the clipboard using names given in the outputKeys.
        It may also do pipeline-specific tweaks to the objects.

    Policy Dictionary:
    datarel/policy/FixupStageDictionary.paf
    """
    def setup(self):
        self.log = Log(self.log, "FixupStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("datarel",
                "FixupStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

    def process(self, clipboard):
        """
        Clear the clipboard of everything except inputKeys, which are remapped
        to outputKeys.
        """
        self.log.log(Log.INFO, "Fixing up the clipboard")
        
        inputKeys = self.policy.get("inputKeys")
        save = {}
        for k in inputKeys.paramNames(True):
            save[k] = clipboard.get(inputKeys.get(k))

        clipboard.close()

        outputKeys = self.policy.get("outputKeys")
        for k in outputKeys.paramNames(True):
            clipboard.put(outputKeys.get(k), save[k])

class FixupStage(harnessStage.Stage):
    parallelClass = FixupStageParallel
