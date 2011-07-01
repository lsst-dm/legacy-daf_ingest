#!/usr/bin/env python

from __future__ import with_statement
from optparse import OptionParser
import sys

def pipelinePolicy(f):
    print >>f, """#<?cfg paf policy ?>

# Unified DC3b PT1.2 main ISR-SFM pipeline policy
framework: {
    type: standard
    environment: "$DATAREL_DIR/bin/runOrca/imsim-setupForOrcaUse.sh"
    exec: "$PEX_HARNESS_DIR/bin/launchPipeline.py"
}

execute: {
    nSlices: 1
    barrierDelay: 0.000001
    localLogMode: true
    eventBrokerHost: lsst8.ncsa.uiuc.edu
    shutdownTopic: shutdownMain
    dir: {
        shortName: Main
        # Following directory info is OVERLAID
        # by imsim-orca.paf:platform defn
        defaultRoot: .
        runDirPattern: "../../%(runid)s/%(shortname)s"
        work: work
        input: input
        output: output
        update: update
        scratch: scratch
    }"""

def jobStart(f):
    print >>f, """
    appStage: {
        name: getAJob
        parallelClass: lsst.ctrl.sched.pipeline.GetAJobParallelProcessing
        eventTopic: None
        stagePolicy: {
            pipelineEvent: CcdJob
        }
    }"""

def isrProcess(f, doJobOffice=False):
    print >>f, """
    appStage: {
        name: isrInputRaw
        parallelClass: lsst.pex.harness.IOStage.InputStageParallel
        eventTopic: """ + ("None" if doJobOffice else "jobIdentity") + """
        stagePolicy: {
            parameters: {
                butler: @PT1Pipe/butlerInput.paf
                inputItems: {"""
    for channelX in (0, 1):
        for channelY in (0, 1, 2, 3, 4, 5, 6, 7):
            for snap in (0, 1):
                channelName = '"%d,%d"' % (channelX, channelY)
                channelSnap = "%d%d_%d" % (channelX, channelY, snap)
                print >>f, """
                    isrExposure""" + channelSnap + """: {
                        datasetType: raw
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: """ + str(snap) + """
                                channel: """ + channelName + """
                            }
                        }
                    }"""
    print >>f, """
                }
            }
        }
    }"""
    for channelX in (0, 1):
        for channelY in (0, 1, 2, 3, 4, 5, 6, 7):
            channelName = '"%d,%d"' % (channelX, channelY)
            channelId = "%d%d" % (channelX, channelY)
            print >>f, """
    appStage: {
        name: isrInput""" + channelId + """
        parallelClass: lsst.pex.harness.IOStage.InputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: @PT1Pipe/butlerInput.paf
                inputItems: {
                    biasExposure: {
                        datasetType: bias
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                channel: """ + channelName + """
                            }
                        }
                    }
                    darkExposure: {
                        datasetType: dark
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                channel: """ + channelName + """
                            }
                        }
                    }
                    flatExposure: {
                        datasetType: flat
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                channel: """ + channelName + """
                            }
                        }
                    }
                }
            }
        }
    }"""
            for snap in (0, 1):
                channelSnap = "%d%d_%d" % (channelX, channelY, snap)
                print >>f, """
    appStage: {
        name: isrSaturation""" + channelSnap + """
        parallelClass: lsst.ip.pipeline.IsrSaturationStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrExposure""" + channelSnap + """
            }
            outputKeys: {
                saturationMaskedExposure: isrExposure""" + channelSnap + """
            }
        }
    }
    appStage: {
        name: isrOverscan""" + channelSnap + """
        parallelClass: lsst.ip.pipeline.IsrOverscanStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrExposure""" + channelSnap + """
            }
            outputKeys: {
                overscanCorrectedExposure: isrExposure""" + channelSnap + """
            }
        }
    }
    appStage: {
        name: isrBias""" + channelSnap + """
        parallelClass: lsst.ip.pipeline.IsrBiasStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrExposure""" + channelSnap + """
                biasexposure: biasExposure
            }
            outputKeys: {
                biasSubtractedExposure: isrExposure""" + channelSnap + """
            }
        }
    }
    appStage: {
        name: isrVariance""" + channelSnap + """
        parallelClass: lsst.ip.pipeline.IsrVarianceStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrExposure""" + channelSnap + """
            }
            outputKeys: {
                varianceAddedExposure: isrExposure""" + channelSnap + """
            }
        }
    }
    appStage: {
        name: isrDark""" + channelSnap + """
        parallelClass: lsst.ip.pipeline.IsrDarkStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrExposure""" + channelSnap + """
                darkexposure: darkExposure
            }
            outputKeys: {
                darkSubtractedExposure: isrExposure""" + channelSnap + """
            }
        }
    }
    appStage: {
        name: isrFlat""" + channelSnap + """
        parallelClass: lsst.ip.pipeline.IsrFlatStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrExposure""" + channelSnap + """
                flatexposure: flatExposure
            }
            parameters: @PT1Pipe/ISR-flat.paf
            outputKeys: {
                flatCorrectedExposure: isrExposure""" + channelSnap + """
            }
        }
    }
    appStage: {
        name: isrSdqaAmp""" + channelSnap + """
        parallelClass: lsst.sdqa.pipeline.IsrSdqaStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposureKey: isrExposure""" + channelSnap + """
            }
            parameters: @PT1Pipe/ISR-sdqaAmp.paf
            outputKeys: {
                isrPersistableSdqaRatingVectorKey: sdqaRatingVector""" + str(snap) + """
            }
        }
    }"""
                pass # end of snap loop

            print >>f, """
    appStage: {
        name: isrOutput""" + channelId + """
        parallelClass: lsst.pex.harness.IOStage.OutputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: @PT1Pipe/butlerUpdate.paf
                outputItems: {
                    sdqaRatingVector0: {
                        datasetId: {
                            datasetType: sdqaAmp
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: 0
                                channel: """ + channelName + """
                            }
                        }
                    }
                    sdqaRatingVector1: {
                        datasetId: {
                            datasetType: sdqaAmp
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: 1
                                channel: """ + channelName + """
                            }
                        }
                    }
                }
            }
        }
    }"""

def ccdAssemblyProcess(f):
    for snap in (0, 1):
        print >>f, """
    appStage: {
        name: ccdAssemblyCcdList""" + str(snap) + """
        parallelClass: lsst.datarel.ObjectListStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {"""
        for channelX in (0, 1):
            for channelY in (0, 1, 2, 3, 4, 5, 6, 7):
                channelId = "%d%d" % (channelX, channelY)
                channelSnap = "%d%d_%d" % (channelX, channelY, snap)
                print >>f, "            object: isrExposure" + channelSnap
        print >>f, """
            }
            outputKeys: {
                objectList: exposureList""" + str(snap) + """
            }
        }
    }
    appStage: {
        name: ccdAssemblyIsrCcdAssembly""" + str(snap) + """
        parallelClass: lsst.ip.pipeline.IsrCcdAssemblyStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposureList: exposureList""" + str(snap) + """
            }
            outputKeys: {
                assembledCcdExposure: isrExposure""" + str(snap) + """
            }
        }
    }
    appStage: {
        name: ccdAssemblyIsrCcdDefect""" + str(snap) + """
        parallelClass: lsst.ip.pipeline.IsrCcdDefectStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                ccdExposure: isrExposure""" + str(snap) + """
            }
            outputKeys: {
                defectMaskedCcdExposure: isrExposure""" + str(snap) + """
            }
        }
    }
    appStage: {
        name: ccdAssemblyIsrCcdSdqa""" + str(snap) + """
        parallelClass: lsst.ip.pipeline.IsrCcdSdqaStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                ccdExposure: isrExposure""" + str(snap) + """
            }
            outputKeys: {
                sdqaCcdExposure: isrExposure""" + str(snap) + """
            }
        }
    }
    appStage: {
        name: ccdAssemblySdqaCcd""" + str(snap) + """
        parallelClass: lsst.sdqa.pipeline.IsrSdqaStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposureKey: isrExposure""" + str(snap) + """
            }
            parameters: @PT1Pipe/ISR-sdqaCcd.paf
            outputKeys: {
                isrPersistableSdqaRatingVectorKey: sdqaRatingVector""" + str(snap) + """
            }
        }
    }"""
    print >>f, """
    appStage: {
        name: ccdAssemblyOutput
        parallelClass: lsst.pex.harness.IOStage.OutputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: @PT1Pipe/butlerUpdate.paf
                outputItems: {"""
    for snap in (0, 1):
        for channelX in (0, 1):
            for channelY in (0, 1, 2, 3, 4, 5, 6, 7):
                channelName = '"%d,%d"' % (channelX, channelY)
                channelSnap = "%d%d_%d" % (channelX, channelY, snap)
                print >>f, """
                    isrExposure""" + channelSnap + """: {
                        datasetId: {
                            datasetType: postISR
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: """ + str(snap) + """
                                channel: """ + channelName + """
                            }
                        }
                    }"""
        print >>f, """
                    isrCcdExposure""" + str(snap) + """: {
                        datasetId: {
                            datasetType: postISRCCD
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: """ + str(snap) + """
                            }
                        }
                    }"""
        
    print >>f, """
                    sdqaRatingVector0: {
                        datasetId: {
                            datasetType: sdqaCcd
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: 0
                            }
                        }
                    }
                    sdqaRatingVector1: {
                        datasetId: {
                            datasetType: sdqaCcd
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: 1
                            }
                        }
                    }
                }
            }
        }
    }"""
    print >>f, """
    appStage: {
        name: ccdAssemblyFixup
        parallelClass: lsst.datarel.FixupStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                isrCcdExposure0: isrExposure0
                isrCcdExposure1: isrExposure1
                jobIdentity: jobIdentity
                originatorId: originatorId
                targetDatasets: targetDatasets
                inputDataSets: inputDatasets
                outputDatasets: outputDatasets
            }
            parameters: {
                pipeline: CcdAssembly
            }
            outputKeys: {
                isrCcdExposure0: isrCcdExposure0
                isrCcdExposure1: isrCcdExposure1
                jobIdentity: jobIdentity
                originatorId: originatorId
                targetDatasets: targetDatasets
                inputDataSets: inputDatasets
                outputDatasets: outputDatasets
            }
        }
    }"""

def crSplitProcess(f):
    print >>f, """
    appStage: {
        name: crSplitBackgroundEstimation0
        parallelClass: lsst.meas.pipeline.BackgroundEstimationStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrCcdExposure0
            }
            outputKeys: {
                backgroundSubtractedExposure: bkgSubCcdExposure0
            }
            parameters: @PT1Pipe/CrSplit-backgroundEstimation.paf
        }
    }
    appStage: {
        name: crSplitBackgroundEstimation1
        parallelClass: lsst.meas.pipeline.BackgroundEstimationStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrCcdExposure1
            }
            outputKeys: {
                backgroundSubtractedExposure: bkgSubCcdExposure1
            }
            parameters: @PT1Pipe/CrSplit-backgroundEstimation.paf
        }
    }
    appStage: {
        name: crSplitCrReject0
        parallelClass: lsst.ip.pipeline.CrRejectStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: bkgSubCcdExposure0
            }
            outputKeys: {
                exposure: crSubCcdExposure0
            }
            parameters: @PT1Pipe/CrSplit-crReject.paf
            crRejectPolicy: @PT1Pipe/CrSplit-crReject-algorithm.paf
        }
    }
    appStage: {
        name: crSplitCrReject1
        parallelClass: lsst.ip.pipeline.CrRejectStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: bkgSubCcdExposure1
            }
            outputKeys: {
                exposure: crSubCcdExposure1
            }
            parameters: @PT1Pipe/CrSplit-crReject.paf
            crRejectPolicy: @PT1Pipe/CrSplit-crReject-algorithm.paf
        }
    }
    """
    print >>f, """
    appStage: {
        name: crSplitFixup
        parallelClass: lsst.datarel.FixupStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                visitExposure: crSubCcdExposure0
                jobIdentity: jobIdentity
                originatorId: originatorId
                targetDatasets: targetDatasets
                inputDataSets: inputDatasets
                outputDatasets: outputDatasets
            }
            parameters: {
                pipeline: CrSplit
            }
            outputKeys: {
                visitExposure: visitExposure
                jobIdentity: jobIdentity
                originatorId: originatorId
                targetDatasets: targetDatasets
                inputDataSets: inputDatasets
                outputDatasets: outputDatasets
            }
        }
    }"""

def imgCharProcess(f):
    print >>f, """
    appStage: {
        name: icSourceDetect
        parallelClass: lsst.meas.pipeline.SourceDetectionStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: visitExposure
            }
            outputKeys: {
                positiveDetection: positiveFootprintSet
                negativeDetection: negativeFootprintSet
                psf: simplePsf
            }
            psfPolicy: @PT1Pipe/ImgChar-sourceDetect-psf.paf
            backgroundPolicy: @PT1Pipe/ImgChar-sourceDetect-background.paf
        }
    }
    appStage: {
        name: icSourceMeasure
        parallelClass: lsst.meas.pipeline.SourceMeasurementStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: visitExposure
                psf: simplePsf
                positiveDetection: positiveFootprintSet
                negativeDetection: negativeFootprintSet
            }
            outputKeys: {
                sources: sourceSet
            }
        }
    }
    appStage: {
        name: icPsfDetermination
        parallelClass: lsst.meas.pipeline.PsfDeterminationStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: visitExposure
                sourceSet: sourceSet
            }
            outputKeys: {
                psf: measuredPsf
                cellSet: cellSet
                sdqa: sdqa
            }
        }
    }
    appStage: {
        name: icRemeasure
        parallelClass: lsst.meas.pipeline.SourceMeasurementPsfFluxStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: visitExposure
                sourceSet: sourceSet
            }
            outputKeys: {
                sourceSet: sourceSet
            }
        }
    }
    appStage: {
        name: icApCorrect
        parallelClass: lsst.meas.pipeline.ApertureCorrectionStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: visitExposure
                cellSet: cellSet
            }
            outputKeys: {
                apCorr: apCorr
                sdqa: sdqa
            }
        }
    }
    appStage: {
        name: icApCorrectApply
        parallelClass: lsst.meas.pipeline.ApertureCorrectionApplyStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                apCorr: apCorr
                sourceSet: sourceSet
            }
        }
    }
    appStage: {
        name: icWcsDetermination
        parallelClass: lsst.meas.pipeline.WcsDeterminationStageParallel
        eventTopic: None
        stagePolicy: @PT1Pipe/ImgChar-wcsDetermination.paf
    }
    appStage: {
        name: icWcsVerification
        parallelClass: lsst.meas.pipeline.WcsVerificationParallel
        eventTopic: None
        stagePolicy: {
            sourceMatchSetKey: matchList
        }
    }
    appStage: {
        name: icPhotoCal
        parallelClass: lsst.meas.pipeline.PhotoCalStageParallel
        eventTopic: None
        stagePolicy: {
            sourceMatchSetKey: matchList
            outputValueKey: photometricMagnitudeObject
        }
    }"""
    print >>f, """
    appStage: {
        name: icOutput
        parallelClass: lsst.pex.harness.IOStage.OutputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: @PT1Pipe/butlerUpdate.paf
                outputItems: {
                    sourceSet_persistable: {
                        datasetId: {
                            datasetType: icSrc
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                    matchList_persistable: {
                        datasetId: {
                            datasetType: icMatch
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                    measuredPsf: {
                        datasetId: {
                            datasetType: psf
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                    apCorr: {
                        datasetId: {
                            datasetType: apCorr
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                    visitExposure: {
                        datasetId: {
                            datasetType: calexp
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                }
            }
        }
    }"""
    print >>f, """
    appStage: {
        name: icFixup
        parallelClass: lsst.datarel.FixupStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                calibratedExposure: visitExposure
                psf: measuredPsf
                jobIdentity: jobIdentity
                apCorr: apCorr
                originatorId: originatorId
                targetDatasets: targetDatasets
                inputDataSets: inputDatasets
                outputDatasets: outputDatasets
            }
            parameters: {
                pipeline: ImgChar
            }
            outputKeys: {
                calibratedExposure: scienceExposure
                psf: psf
                jobIdentity: jobIdentity
                apCorr: apCorr
                originatorId: originatorId
                targetDatasets: targetDatasets
                inputDataSets: inputDatasets
                outputDatasets: outputDatasets
            }
        }
    }"""

def sfmProcess(f):
    print >>f, """
    appStage: {
        name: sfmSourceDetect
        parallelClass: lsst.meas.pipeline.SourceDetectionStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: scienceExposure
                psf: psf
            }
            outputKeys: {
                positiveDetection: positiveFootprintSet
            }
            backgroundPolicy: @PT1Pipe/SFM-sourceDetect-background.paf
        }
    }
    appStage: {
        name: sfmSourceMeasure
        parallelClass: lsst.meas.pipeline.SourceMeasurementStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: scienceExposure
                psf: psf
                positiveDetection: positiveFootprintSet
            }
            outputKeys: {
                sources: sourceSet
            }
            measureSources: @PT1Pipe/SFM-sourceMeasure.paf
        }
    }
    appStage: {
        name: sfmApCorrectApply
        parallelClass: lsst.meas.pipeline.ApertureCorrectionApplyStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                apCorr: apCorr
                sourceSet: sourceSet
            }
        }
    }
    appStage: {
        name: sfmComputeSourceSkyCoords
        parallelClass: lsst.meas.pipeline.ComputeSourceSkyCoordsStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                sources: sourceSet
                exposure: scienceExposure
            }
        }
    }"""
    print >>f, """
    appStage: {
        name: sfmOutput
        parallelClass: lsst.pex.harness.IOStage.OutputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: @PT1Pipe/butlerUpdateFootprints.paf
                outputItems: {
                    sourceSet_persistable: {
                        datasetId: {
                            datasetType: src
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                }
            }
        }
    }"""

def jobFinish(f):
    print >>f, """
    appStage: {
        name: jobDone
        parallelClass: lsst.ctrl.sched.pipeline.JobDoneParallelProcessing
        eventTopic: None
        stagePolicy: {
            pipelineEvent:  CcdJob
            datasets.dataReadyEvent:  CalexpAvailable
        }
    }
    failureStage: {
        name: failure
        parallelClass: lsst.ctrl.sched.pipeline.JobDoneParallelProcessing
        eventTopic: None
        stagePolicy: {
            pipelineEvent:  CcdJob
            datasets.dataReadyEvent:  CalexpAvailable
            jobSuccess: false
        }
    }"""


###############################################################################

def createPolicy(f, doJobOffice=False):
    pipelinePolicy(f)
    if doJobOffice:
        jobStart(f)
    isrProcess(f, doJobOffice)
    ccdAssemblyProcess(f)
    crSplitProcess(f)
    imgCharProcess(f)
    sfmProcess(f)
    if doJobOffice:
        jobFinish(f)
    print >>f, "}"

def main():
    parser = OptionParser()
    parser.add_option("-j", "--jobOffice", action="store_true",
            default=False, help="write JobOffice stages into the pipeline")
    options, args = parser.parse_args()
    if len(args) > 0:
        with open(args[0], "w") as f:
            createPolicy(f, options.jobOffice)
    else:
        createPolicy(sys.stdout, options.jobOffice)

if __name__ == "__main__":
    main()
