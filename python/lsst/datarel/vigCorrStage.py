#!/usr/bin/env python
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy

class VigCorrStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage applies a vignetting correction
        to a background-subtracted CCD image
        by multiplying (or optionally dividing)
        the background-subtracted image by a correction image

    Policy Dictionary:
    datarel/policy/VigCorrStageDictionary.paf
    """
    def setup(self):
        self.log = Log(self.log, "VigCorrStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("datarel",
                "VigCorrStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())
        self._doCorrect = self.policy.get("doCorrect")
        self._doMultiply = self.policy.get("doMultiply")

    def process(self, clipboard):
        """
        
        Clear the clipboard of everything except inputKeys, which are remapped
        to outputKeys.
        """
        inputKeys = self.policy.get("inputKeys")
        exposure = clipboard.get(inputKeys.get("exposure"))
        vigCorrImage = clipboard.get(inputKeys.get("vigCorrImage"))
        
        corrExposure = afwImage.ExposureF(exposure, true)
        corrExposure.setCalib(exposure.getCalib())
        corrExposure.setDetector(exposure.getDetector())
        corrExposure.setFilter(exposure.getFilter())
        
        if self._doCorrect:
            if self._doMultiply:
                self.log.log(Log.INFO, "corrExposure = exposure * vigCorrImage")
                vigCorrImage *= vigCorrImage
            else:
                self.log.log(Log.INFO, "corrExposure = exposure / vigCorrImage")
                vigCorrImage /= vigCorrImage
        else:
            self.log.log(Log.INFO, "corrExposure = exposure (no correction)")
        
        outputKeys = self.policy.get("outputKeys")
        clipboard.put(corrExposureName, outputKeys.get("corrExposure"))

class VigCorrStage(harnessStage.Stage):
    parallelClass = VigCorrStageParallel