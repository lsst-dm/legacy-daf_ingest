#!/usr/bin/env python

from __future__ import with_statement
import sys


def pipelinePolicy(f):
    print >>f, """#<?cfg paf policy ?>

# Unified DC3b PT1.1 main ISR-SFM pipeline policy
framework: {
    type: standard
    environment: "$DATAREL_DIR/bin/runOrca/imsim-setupForOrcaUse.sh"
    exec: "$PEX_HARNESS_DIR/bin/launchPipeline.py"
}

execute: {
    nSlices: 1
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

def isrProcess(f):
    print >>f, """
    appStage: {
        name: isrInputRaw
        parallelClass: lsst.pex.harness.IOStage.InputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: {
                    mapperName: lsst.obs.lsstSim.lsstSimMapper
                    mapperPolicy: {
                        root: %(input)
                    }
                }
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
                butler: {
                    mapperName: lsst.obs.lsstSim.lsstSimMapper
                    mapperPolicy: {
                        root: %(input)
                    }
                }
                inputItems: {
                    biasExposure: {
                        datasetType: bias
                        datasetId: {
                            fromJobIdentity: "raft" "sensor"
                            set: {
                                channel: """ + channelName + """
                            }
                        }
                    }
                    darkExposure: {
                        datasetType: dark
                        datasetId: {
                            fromJobIdentity: "raft" "sensor"
                            set: {
                                channel: """ + channelName + """
                            }
                        }
                    }
                    flatExposure: {
                        datasetType: flat
                        datasetId: {
                            fromJobIdentity: "raft" "sensor"
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
            outputKeys: {
                darkSubtractedExposure: isrExposure""" + channelSnap + """
            }
            parameters: {
                flatScalingValue: 1.0
            }
            outputKeys: {
                flatCorrectedExposure: isrExposure""" + channelSnap + """
            }
        }
    }
    appStage: {
        name: isrSdqa""" + channelSnap + """
        parallelClass: lsst.sdqa.pipeline.IsrSdqaStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrExposure""" + channelSnap + """
            }
            parameters: {
                sdqaRatingScope: 0
                sdqaMetricNames: "overscanMean"
                sdqaMetricNames: "overscanMedian"
                sdqaMetricNames: "overscanStdDev"
                sdqaMetricNames: "overscanMin"
                sdqaMetricNames: "overscanMax"
            }
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
                butler: {
                    mapperName: lsst.obs.lsstSim.lsstSimMapper
                    mapperPolicy: {
                        root: %(update)
                    }
                }
                outputItems: {
                    sdqaRatingVector0: {
                        datasetType: sdqaAmp
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: 0
                                channel: """ + channelName + """
                            }
                        }
                    }
                    sdqaRatingVector1: {
                        datasetType: sdqaAmp
                        datasetId: {
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
        name: ccdList""" + str(snap) + """
        parallelClass: lsst.datarel.CcdListStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {"""
        for channelX in (0, 1):
            for channelY in (0, 1, 2, 3, 4, 5, 6, 7):
                channelId = "%d%d" % (channelX, channelY)
                channelSnap = "%d%d_%d" % (channelX, channelY, snap)
                print >>f, """
                exposure""" + channelId + ": isrExposure" + channelSnap
        print >>f, """
            }
            outputKeys: {
                exposureList: exposureList""" + str(snap) + """
            }
        }
    }
    appStage: {
        name: isrCcdAssembly""" + str(snap) + """
        parallelClass: lsst.ip.pipeline.IsrCcdAssemblyStageParallel
        eventTopic: None
        outputKeys: {
            assembledCcdExposure: isrExposure""" + str(snap) + """
        }
    }
    appStage: {
        name: isrCcdDefect""" + str(snap) + """
        parallelClass: lsst.ip.pipeline.IsrCcdDefectStageParallel
        eventTopic: None
        inputKeys: {
            ccdExposure: isrExposure""" + str(snap) + """
        }
        outputKeys: {
            ccdExposure: isrExposure""" + str(snap) + """
        }
    }
    appStage: {
        name: isrCcdSdqa""" + str(snap) + """
        parallelClass: lsst.ip.pipeline.IsrCcdSdqaStageParallel
        eventTopic: None
        inputKeys: {
            ccdExposure: isrExposure""" + str(snap) + """
        }
        outputKeys: {
            sdqaCcdExposure: isrExposure""" + str(snap) + """
        }
    }
    appStage: {
        name: isrSdqa""" + str(snap) + """
        parallelClass: lsst.sdqa.pipeline.IsrSdqaStageParallel
        eventTopic: None
        inputKeys: {
            exposureKey: isrExposure""" + str(snap) + """
        }
        parameters: {
            sdqaRatingScope: 1
            sdqaMetricNames: "imageClipMean4Sig3Pass"
            sdqaMetricNames: "imageMedian"
            sdqaMetricNames: "imageSigma"
            sdqaMetricNames: "nBadCalibPix"
            sdqaMetricNames: "nSaturatePix"
            sdqaMetricNames: "imageMin"
            sdqaMetricNames: "imageMax"
        }
        outputKeys: {
            isrPersistableSdqaRatingVectorKey: sdqaRatingVector""" + str(snap) + """
        }
    }"""
    print >>f, """
    appStage: {
        name: ccdAssemblyOutput
        parallelClass: lsst.pex.harness.IOStage.OutputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: {
                    mapperName: lsst.obs.lsstSim.lsstSimMapper
                    mapperPolicy: {
                        root: %(update)
                    }
                }
                outputItems: {
                    sdqaRatingVector0: {
                        datasetType: sdqaCcd
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                            set: {
                                snap: 0
                            }
                        }
                    }
                    sdqaRatingVector1: {
                        datasetType: sdqaCcd
                        datasetId: {
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
        parallelClass: lsst.datarel.CcdAssemblyFixupStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                isrCcdExposure0: isrExposure0
                isrCcdExposure1: isrExposure1
            }
            # Set exposure midtimes
            # Delete all other items on the clipboard
            outputKeys: {
                isrCcdExposure0: isrCcdExposure0
                isrCcdExposure1: isrCcdExposure1
            }
        }
    }"""

def crSplitProcess(f):
    print >>f, """
    appStage: {
        name: backgroundEstimation0
        parallelClass: lsst.meas.pipeline.BackgroundEstimationStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrCcdExposure0
            }
            outputKeys: {
                backgroundSubtractedExposure: bkgSubCcdExposure0
            }
            parameters: {
                subtractBackground: true
                backgroundPolicy: {
                    binsize: 512
                }
            }
    }
    appStage: {
        name: backgroundEstimation1
        parallelClass: lsst.meas.pipeline.BackgroundEstimationStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: isrCcdExposure1
            }
            outputKeys: {
                backgroundSubtractedExposure: bkgSubCcdExposure1
            }
            parameters: {
                subtractBackground: true
                backgroundPolicy: {
                    binsize: 512
                }
            }
        }
    }
    appStage: {
        name: crReject0
        parallelClass: lsst.ip.pipeline.CrRejectStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: bkgSubCcdExposure0
            }
            outputKeys: {
                exposure: visitim
                exposure: crSubCcdExposure0
            }
            parameters: {
                defaultFwhm: 1.0
                keepCRs: false
            }
            crRejectPolicy: {
                nCrPixelMax: 100000
            }
        }
    }
    appStage: {
        name: crReject1
        parallelClass: lsst.ip.pipeline.CrRejectStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                exposure: bkgSubCcdExposure1
            }
            outputKeys: {
                exposure: crSubCcdExposure1
            }
            parameters: {
                defaultFwhm: 1.0
                keepCRs: false
            }
            crRejectPolicy: {
                nCrPixelMax: 100000
            }
        }
    }
    """
    print >>f, """
    appStage: {
        name: crRejectFixup
        parallelClass: lsst.datarel.CrRejectFixupStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                visitExposure: crSubCcdExposure0
            }
            # Handle any processing due to use of only one snap
            # Delete all other items on the clipboard
            outputKeys: {
                visitExposure: visitExposure
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
            psfPolicy: {
                height: 5
                width: 5
                parameter: 1.0
            }
            backgroundPolicy: {
                algorithm: NONE
            }
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
        name: psfDetermination
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
        name: wcsDetermination
        parallelClass: lsst.meas.pipeline.WcsDeterminationStageParallel
        eventTopic: None
        stagePolicy: {
            inputExposureKey: visitExposure
            inputSourceSetKey: sourceSet
            outputWcsKey: measuredWcs
            outputMatchListKey: matchList
            numBrightStars: 150
            defaultFilterName: mag
        }
    }
    appStage: {
        name: wcsVerification
        parallelClass: lsst.meas.pipeline.WcsVerificationStageParallel
        eventTopic: None
        stagePolicy: {
            sourceMatchSetKey: matchList
        }
    }
    appStage: {
        name: photoCal
        parallelClass: lsst.meas.pipeline.PhotoCalStageParallel
        eventTopic: None
        stagePolicy: {
            sourceMatchSetKey: matchList
            outputValueKey: photometricMagnitudeObject
        }
    }"""
    print >>f, """
    appStage: {
        name: imgCharOutput
        parallelClass: lsst.pex.harness.IOStage.OutputStageParallel
        eventTopic: None
        stagePolicy: {
            parameters: {
                butler: {
                    mapperName: lsst.obs.lsstSim.lsstSimMapper
                    mapperPolicy: {
                        root: %(update)
                    }
                }
                outputItems: {
                    sourceSet_persistable: {
                        datasetType: icSrc
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                    measuredPsf: {
                        datasetType: psf
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                    visitExposure: {
                        datasetType: calexp
                        datasetId: {
                            fromJobIdentity: "visit" "raft" "sensor"
                        }
                    }
                }
            }
        }
    }"""
    print >>f, """
    appStage: {
        name: imgCharCleanup
        parallelClass: lsst.datarel.CleanupStageParallel
        eventTopic: None
        stagePolicy: {
            inputKeys: {
                calibratedExposure: visitExposure
                psf: measuredPsf
            }
            # Delete all other items on the clipboard
            outputKeys: {
                calibratedExposure: scienceExposure
                psf: psf
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
            backgroundPolicy: {
                algorithm: NONE
            }
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
        }
    }
    appStage: {
        name: computeSourceSkyCoords
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
                butler: {
                    mapperName: lsst.obs.lsstSim.lsstSimMapper
                    mapperPolicy: {
                        root: %(update)
                    }
                }
                outputItems: {
                    sourceSet_persistable: {
                        datasetType: src
                        datasetId: {
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

def createPolicy(f):
    pipelinePolicy(f)
    jobStart(f)
    isrProcess(f)
    ccdAssemblyProcess(f)
    crSplitProcess(f)
    imgCharProcess(f)
    sfmProcess(f)
    jobFinish(f)
    print >>f, "}"

def main():
    if len(sys.argv) > 1: 
        with open(sys.argv[1], "w") as f:
            createPolicy(f)
    else:
        createPolicy(sys.stdout)

if __name__ == "__main__":
    main()
