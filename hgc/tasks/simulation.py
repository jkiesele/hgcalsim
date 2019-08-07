# coding: utf-8

"""
HGCAL simulation tasks.
"""


__all__ = ["GSDTask", "RecoTask", "NtupTask"]


import os
import random

import law
import luigi

from hgc.tasks.base import Task, HTCondorWorkflow
from hgc.util import cms_run_and_publish, log_runtime


luigi.namespace("sim", scope=__name__)


class GeneratorParameters(Task):

    n_events = luigi.IntParameter(default=10, description="number of events to generate per task")
    n_tasks = luigi.IntParameter(default=1, description="number of branch tasks to create")
    gun_type = luigi.ChoiceParameter(default="closeby", choices=["flatpt", "closeby"],
        description="the type of the particle gun")
    gun_min = luigi.FloatParameter(default=1.0, description="minimum value of the gun, either in "
        "pt or E, default: 1.0")
    gun_max = luigi.FloatParameter(default=100.0, description="maximum value of the gun, either in "
        "pt or E, default: 100.0")
    particle_ids = luigi.Parameter(default="mix", description="comma-separated list of particle "
        "ids to shoot, or 'mix', default: mix")
    delta_r = luigi.FloatParameter(default=0.1, description="distance parameter, 'closeby' gun "
        "only, default: 0.1")
    n_particles = luigi.IntParameter(default=10, description="number of particles to shoot, "
        "'closeby' gun only, default: 10")
    exact_shoot = luigi.BoolParameter(default=False, description="shoot exactly the particles "
        "given particle-ids in that order and quantity, 'closeby' gun only, default: False")
    random_shoot = luigi.BoolParameter(default=True, description="shoot a random number of "
        "particles between [1, n_particles], 'closeby' gun only, default: True")
    seed = luigi.IntParameter(default=1, description="initial random seed, will be increased by "
        "branch number, default: 1")

    def store_parts(self):
        """
        Method that defines how certain parameters translate into the directory in which output
        files are stored by means of :py:meth:`Task.local_target` and :py:meth:`Task.local_path`,
        respectively.
        """
        parts = super(GeneratorParameters, self).store_parts()

        # build the gun string
        assert(self.gun_type in ("flatpt", "closeby"))
        gun_str = "{}_{}To{}_ids{}".format(self.gun_type, self.gun_min, self.gun_max,
            self.particle_ids.replace(",", "-"))
        if self.gun_type == "closeby":
            gun_str += "_dR{}_n{}".format(self.delta_r, self.n_particles)
            # add the exact or random shoot postfix for backwards compatibility
            if self.exact_shoot:
                gun_str += "_ext1"
            else:
                gun_str += "_rnd{:d}".format(self.random_shoot)
        gun_str += "_s{}".format(self.seed)

        return parts + (gun_str,)


class ParallelProdWorkflow(GeneratorParameters, law.LocalWorkflow, HTCondorWorkflow):

    previous_task = None

    def create_branch_map(self):
        return {i: i for i in range(self.n_tasks)}

    def workflow_requires(self):
        reqs = super(ParallelProdWorkflow, self).workflow_requires()
        if self.previous_task and not self.pilot:
            key, cls = self.previous_task
            reqs[key] = cls.req(self, _prefer_cli=["version"])
        return reqs

    def requires(self):
        reqs = {}
        if self.previous_task:
            key, cls = self.previous_task
            reqs[key] = cls.req(self, _prefer_cli=["version"])
        return reqs


class GSDTask(ParallelProdWorkflow):

    def output(self):
        return self.local_target("gsd_{}_n{}.root".format(self.branch, self.n_events))

    @law.decorator.localize()
    def run(self):
        # run the command using a helper that publishes the current progress to the scheduler
        cms_run_and_publish(self, "$HGC_BASE/hgc/files/gsd_cfg.py", dict(
            outputFile=self.output().path,
            maxEvents=self.n_events,
            gunType=self.gun_type,
            gunMin=self.gun_min,
            gunMax=self.gun_max,
            particleIds=self.particle_ids,
            deltaR=self.delta_r,
            nParticles=self.n_particles,
            exactShoot=self.exact_shoot,
            randomShoot=self.random_shoot,
            seed=self.seed + self.branch,
        ))


class RecoTask(ParallelProdWorkflow):

    previous_task = ("gsd", GSDTask)

    def output(self):
        return {
            "reco": self.local_target("reco_{}_n{}.root".format(self.branch, self.n_events)),
            "dqm": self.local_target("dqm_{}_n{}.root".format(self.branch, self.n_events)),
        }

    @law.decorator.localize()
    def run(self):
        inp = self.input()
        outp = self.output()

        cms_run_and_publish(self, "$HGC_BASE/hgc/files/reco_cfg.py", dict(
            inputFiles=[inp["gsd"].path],
            outputFile=outp["reco"].path,
            outputFileDQM=outp["dqm"].path,
        ))


class NtupTask(ParallelProdWorkflow):

    previous_task = ("reco", RecoTask)

    def output(self):
        return self.local_target("ntup_{}_n{}.root".format(self.branch, self.n_events))

    @law.decorator.localize()
    def run(self):
        inp = self.input()
        outp = self.output()

        cms_run_and_publish(self, "$HGC_BASE/hgc/files/ntup_cfg.py", dict(
            inputFiles=[inp["reco"]["reco"].path],
            outputFile=outp.path,
        ))
