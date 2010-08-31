#!/usr/bin/env python
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy

class ObjectListStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage is used to group separate objects on the clipboard into a
        single Python list.

    Policy Dictionary:
    datarel/policy/ObjectListStageDictionary.paf
    """
    def setup(self):
        self.log = Log(self.log, "ObjectListStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("datarel",
                "ObjectListStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

    def process(self, clipboard):
        """
        Group all the objects specified by inputKeys.object into a single list
        specified by outputKeys.objectList.
        """
        self.log.log(Log.INFO, "Grouping objects into list")
        
        objectNames = self.policy.getArray("inputKeys.object")
        objectList = []
        for k in self.policy.getArray("inputKeys.object"):
            objectList.append(clipboard.get(k))

        clipboard.put(self.policy.get("outputKeys.objectList"), objectList)

class ObjectListStage(harnessStage.Stage):
    parallelClass = ObjectListStageParallel
