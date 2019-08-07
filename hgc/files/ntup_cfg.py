# coding: utf-8
# flake8: noqa

"""
HGCAL NTUP config.
"""


import FWCore.ParameterSet.Config as cms
from reco_prodtools.templates.NTUP_fragment import process
from FWCore.ParameterSet.VarParsing import VarParsing


# options
options = VarParsing("python")

# set defaults of common options
options.setDefault("outputFile", "ntup.root")
options.setDefault("maxEvents", -1)

options.parseArguments()


# input / output
process.maxEvents.input = cms.untracked.int32(-1)
process.source.fileNames = cms.untracked.vstring(
    *["file:{}".format(f) for f in options.inputFiles])
process.TFileService = cms.Service("TFileService",
    fileName=cms.string("file:{}".format(options.__getattr__("outputFile", noTags=True))))


# setup the HGCAL tuple writer
from FastSimulation.Event.ParticleFilter_cfi import *
from RecoLocalCalo.HGCalRecProducers.HGCalRecHit_cfi import dEdX

process.ana = cms.EDAnalyzer("HGCalAnalysis",
    detector=cms.string("all"),
    inputTag_HGCalMultiCluster=cms.string("hgcalMultiClusters"),
    rawRecHits=cms.bool(True),
    verbose=cms.bool(False),
    readCaloParticles=cms.bool(True),
    readGenParticles=cms.bool(False),
    storeGenParticleOrigin=cms.bool(False),
    storeGenParticleExtrapolation=cms.bool(False),
    storePCAvariables=cms.bool(False),
    storeElectrons=cms.bool(True),
    storePFCandidates=cms.bool(False),
    storeGunParticles=cms.bool(True),
    recomputePCA=cms.bool(False),
    includeHaloPCA=cms.bool(True),
    dEdXWeights=dEdX.weights,
    layerClusterPtThreshold=cms.double(-1),
    TestParticleFilter=ParticleFilterBlock.ParticleFilter.clone(
        protonEMin=cms.double(100000),
        etaMax=cms.double(3.1),
    ),
)

# remove all registered paths and the schedule,
# so that only our ntuplizer paths will be executed
for p in process.paths:
    delattr(process, p)
delattr(process, "schedule")

# define the schedule, also re-cluster
process.p = cms.Path(process.hgcalLayerClusters + process.ana)
process.schedule = cms.Schedule(process.p)
