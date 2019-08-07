# coding: utf-8
# flake8: noqa

"""
HGCAL GEN, SIM and DIGI config.
"""


import math

import FWCore.ParameterSet.Config as cms
from reco_prodtools.templates.GSD_fragment import process
from FWCore.ParameterSet.VarParsing import VarParsing


# constants
HGCAL_Z = 319.0
HGCAL_ETA_MIN = 1.6
HGCAL_ETA_MAX = 3.0


# helpers
def calculate_rho(z, eta):
    return z * math.tan(2 * math.atan(math.exp(-eta)))


# options
options = VarParsing("python")

# set defaults of common options
options.setDefault("outputFile", "gsd.root")
options.setDefault("maxEvents", 1)

# register custom options
options.register("gunType", "closeby", VarParsing.multiplicity.singleton, VarParsing.varType.string,
    "the gun type to use, either 'flatpt' or 'closeby'")
options.register("gunMin", 1.0, VarParsing.multiplicity.singleton, VarParsing.varType.float,
    "the minimum gun value, i.e., pt for 'flatpt' or E for 'closeby' gun")
options.register("gunMax", 100.0, VarParsing.multiplicity.singleton, VarParsing.varType.float,
    "the maximum gun value, i.e., pt for 'flatpt' or E for 'closeby' gun")
options.register("particleIds", "mix", VarParsing.multiplicity.singleton, VarParsing.varType.string,
    "ids of particles to shoot in a comma-separated list or 'mix'")
options.register("deltaR", 0.1, VarParsing.multiplicity.singleton, VarParsing.varType.float,
    "deltaR parameter, 'closeby' gun only")
options.register("nParticles", 10, VarParsing.multiplicity.singleton, VarParsing.varType.int,
    "number of particles to shoot, 'closeby' gun only")
options.register("exactShoot", False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
    "shoot exactly what is given in particleIds in that order and quantity, 'closeby' gun only")
options.register("randomShoot", False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
    "shoot a random number of particles between [1, nParticles], 'closeby' gun only")
options.register("seed", 1, VarParsing.multiplicity.singleton, VarParsing.varType.int,
    "random seed")

options.parseArguments()


# input / output
process.maxEvents.input = cms.untracked.int32(options.maxEvents)
process.FEVTDEBUGHLToutput.fileName = cms.untracked.string(
    "file:{}".format(options.__getattr__("outputFile", noTags=True)))
process.source.firstLuminosityBlock = cms.untracked.uint32(1)

# random seeds
process.RandomNumberGeneratorService.generator.initialSeed = cms.untracked.uint32(options.seed)
process.RandomNumberGeneratorService.VtxSmeared.initialSeed = cms.untracked.uint32(options.seed)
process.RandomNumberGeneratorService.mix.initialSeed = cms.untracked.uint32(options.seed)

# build the particle id list
if options.particleIds == "mix":
    if options.exactShoot:
        raise Exception("when exactShoot is True, particleIds must not be 'mix'")
    # particle "mix" is 20% pi+, 20% pi-, 20% gamma, 10% e+, 10% e-, 10% mu+, 10% mu-
    particle_ids = 20 * [211, -211] + 20 * [22] + 10 * [11, -11] + 10 * [13, -13]
else:
    # try to parse a comma-separated list
    try:
        particle_ids = [int(s) for s in options.particleIds.strip().split(",")]
    except ValueError:
        raise ValueError("could not convert list of particle ids '{}' to integers".format(
            options.particleIds))

# gun setup
if options.gunType == "flatpt":
    process.generator = cms.EDProducer("FlatRandomPtGunProducer",
        PGunParameters=cms.PSet(
            # particle ids
            PartID=cms.vint32(particle_ids),
            # pt range
            MinPt=cms.double(options.gunMin),
            MaxPt=cms.double(options.gunMax),
            # phi range
            MinPhi=cms.double(-math.pi / 6.),
            MaxPhi=cms.double(math.pi / 6.),
            # abs eta range
            MinEta=cms.double(HGCAL_ETA_MIN),
            MaxEta=cms.double(HGCAL_ETA_MAX),
        ),
        AddAntiParticle=cms.bool(False),
        Verbosity=cms.untracked.int32(1),
    )

elif options.gunType == "closeby":
    process.generator = cms.EDProducer("CloseByFlatDeltaRGunProducer",
        # particle ids
        particleIDs=cms.vint32(particle_ids),
        # max number of particles to shoot at a time
        nParticles=cms.int32(options.nParticles),
        # shoot exactly the particles defined in particleIDs in that order
        exactShoot=cms.bool(options.exactShoot),
        # randomly shoot [1, nParticles] particles, each time randomly drawn from particleIDs
        randomShoot=cms.bool(options.randomShoot),
        # energy range
        eMin=cms.double(options.gunMin),
        eMax=cms.double(options.gunMax),
        # phi range
        phiMin=cms.double(-math.pi / 6.),
        phiMax=cms.double(math.pi / 6.),
        # longitudinal distance in cm
        zMin=cms.double(HGCAL_Z),
        zMax=cms.double(HGCAL_Z),
        # radial distance in cm
        rhoMin=cms.double(calculate_rho(HGCAL_Z, HGCAL_ETA_MIN)),
        rhoMax=cms.double(calculate_rho(HGCAL_Z, HGCAL_ETA_MAX)),
        # deltaR settings
        deltaRMin=cms.double(0.),
        deltaRMax=cms.double(options.deltaR),
        # debug flag
        debug=cms.untracked.bool(True),
    )

else:
    raise ValueError("unknown gun type '{}', must be 'flatpt' or 'closeby'".format(options.gunType))
