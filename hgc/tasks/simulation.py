# coding: utf-8

"""
HGCAL simulation tasks.
"""


__all__ = ["GSDTask", "RecoTask", "NtupTask", "WindowNtupTask"]


import sys
import os
import random
import math
import collections
import argparse
import contextlib

import law
import luigi
import six

from hgc.tasks.base import Task, HTCondorWorkflow
from hgc.util import cms_run_and_publish, log_runtime


luigi.namespace("sim", scope=__name__)


@contextlib.contextmanager
def import_hgcal_pgun():
    prodtools_path = os.path.expandvars("$CMSSW_BASE/src/reco_prodtools")
    sys.path.insert(0, prodtools_path)

    try:
        import SubmitHGCalPGun
        yield SubmitHGCalPGun

    finally:
        sys.path.pop(0)


class GeneratorParameters(Task):

    n_tasks = luigi.IntParameter(default=1, description="number of branch tasks to create to "
        "parallelize the simulation of the requested number of events")
    seed = luigi.IntParameter(default=1, description="initial random seed, will be increased by "
        "branch number, default: 1")

    prodtools_parser = None
    prodtools_specs = None
    prodtools_skip_opts = ("help", "tag", "queue", "evtsperjob", "inDir", "outDir", "local",
        "dry-run", "eosArea", "cfg", "datTier", "skipInputs", "keepDQMfile")

    @classmethod
    def get_prodtools_parser(cls):
        if not cls.prodtools_parser:
            with import_hgcal_pgun() as SubmitHGCalPGun:
                cls.prodtools_parser = SubmitHGCalPGun.createParser()

        return cls.prodtools_parser

    @classmethod
    def get_prodtools_specs(cls):
        if not cls.prodtools_specs:
            cls.prodtools_specs = collections.OrderedDict()

            for opt in cls.get_prodtools_parser().option_list:
                name = opt.get_opt_string()
                opt_name = name[2:]

                if not name.startswith("--") or opt_name in cls.prodtools_skip_opts:
                    continue

                opt_type = opt.type
                if not opt_type and isinstance(opt.default, bool):
                    opt_type = "bool"

                cls.prodtools_specs[opt_name] = dict(type=opt_type, default=opt.default,
                    help=opt.help, dest=opt.dest or opt_name)

        return cls.prodtools_specs

    def store_parts(self):
        """
        Method that defines how certain parameters translate into the directory in which output
        files are stored by means of :py:meth:`Task.local_target` and :py:meth:`Task.local_path`,
        respectively.
        """
        parts = super(GeneratorParameters, self).store_parts()

        # for the moment, just create a hash of all prodtools parameter values
        param_names = sorted(list(self.get_prodtools_specs().keys()))
        param_hash = law.util.create_hash([getattr(self, attr) for attr in param_names])

        return parts + (param_hash,)


# register prodtool parameters to the GeneratorParameters base task to resemble its interface
for opt_name, opt_data in six.iteritems(GeneratorParameters.get_prodtools_specs()):
    cls = {
        "int": luigi.IntParameter,
        "string": luigi.Parameter,
        "bool": luigi.BoolParameter,
        "float": luigi.FloatParameter,
    }[opt_data["type"]]

    param = cls(default=opt_data["default"], description=opt_data["help"])

    setattr(GeneratorParameters, opt_name, param)


class CreateConfigs(GeneratorParameters):

    def output(self):
        return collections.OrderedDict(
            (tier, self.local_target("{}_cfg.py".format(tier)))
            for tier in ("gsd", "reco", "ntup")
        )

    @law.decorator.localize()
    @law.decorator.safe_output()
    def run(self):
        output = self.output()

        # do the following steps for all data tiers
        for tier, outp in output.items():
            # create a temporary directories to create the files in
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.child("cfg", type="d").touch()
            tmp_dir.child("jobs", type="d").touch()

            # get parameters related to prodtools
            specs = self.get_prodtools_specs()
            params = {spec["dest"]: getattr(self, name) for name, spec in specs.items()}

            # extend by fixed values
            params.update(dict(
                outDir=tmp_dir.path,
                inDir="",
                DTIER=tier.upper(),
                CONFIGFILE="",
                eosArea="",
                LOCAL=True,
                QUEUE="tomorrow",
                DRYRUN=True,
                EVTSPERJOB=self.nevts,
                TAG="",
                skipInputs=True,
                DQM=True,
            ))

            # run submitHGCalProduction
            with import_hgcal_pgun() as SubmitHGCalPGun:
                cfgs = SubmitHGCalPGun.submitHGCalProduction(opt=argparse.Namespace(**params))

            if len(cfgs) != 1:
                raise Exception("SubmitHGCalPGun created {} config files for data tier {}, while 1 "
                    "was expected".format(tier, len(cfgs)))

            # provide the output
            outp.copy_from_local(cfgs[0])


class ParallelProdWorkflow(GeneratorParameters, law.LocalWorkflow, HTCondorWorkflow):

    previous_task = None

    def create_branch_map(self):
        return {i: i for i in range(self.n_tasks)}

    def workflow_requires(self):
        reqs = super(ParallelProdWorkflow, self).workflow_requires()

        # always require the config files for this set of generator parameters
        reqs["cfg"] = CreateConfigs.req(self, _prefer_cli=["version"])

        # add the "previous" task when not piloting
        if self.previous_task and not self.pilot:
            key, cls = self.previous_task
            reqs[key] = cls.req(self, _prefer_cli=["version"])

        return reqs

    def requires(self):
        reqs = {}

        # always require the config files for this set of generator parameters
        reqs["cfg"] = CreateConfigs.req(self, _prefer_cli=["version"])

        # add the "previous" task
        if self.previous_task:
            key, cls = self.previous_task
            reqs[key] = cls.req(self, _prefer_cli=["version"])

        return reqs


class GSDTask(ParallelProdWorkflow):

    def output(self):
        return self.local_target("gsd_{}.root".format(self.branch))

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        inp = self.input()
        outp = self.output()

        # run the command using a helper that publishes the current progress to the scheduler
        cms_run_and_publish(self, inp["cfg"]["gsd"].path, dict(
            outputFile=outp.uri(),
            maxEvents=int(math.ceil(self.nevts / float(self.n_tasks))),
            seed=self.seed + self.branch,
        ))


class RecoTask(ParallelProdWorkflow):

    # set previous_task which ParallelProdWorkflow uses to set the requirements
    previous_task = ("gsd", GSDTask)

    def output(self):
        return {
            "reco": self.local_target("reco_{}.root".format(self.branch)),
            "dqm": self.local_target("dqm_{}.root".format(self.branch)),
        }

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        inp = self.input()
        outp = self.output()

        cms_run_and_publish(self, inp["cfg"]["reco"].path, dict(
            inputFiles=[inp["gsd"].uri()],
            outputFile=outp["reco"].uri(),
            outputFileDQM=outp["dqm"].uri(),
        ))

        # remove GSD input after completion of the reco step
        for inp in law.util.flatten(inp["gsd"]):
            inp.remove()


class NtupTask(ParallelProdWorkflow):

    # set previous_task which ParallelProdWorkflow uses to set the requirements
    previous_task = ("reco", RecoTask)

    def output(self):
        return self.local_target("ntup_{}.root".format(self.branch))

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        inp = self.input()
        outp = self.output()

        cms_run_and_publish(self, inp["cfg"]["ntup"].path, dict(
            inputFiles=[inp["reco"]["reco"].uri()],
            outputFile=outp.uri(),
        ))

        # remove GSD input after completion of the reco step
        for inp in law.util.flatten(inp["reco"]):
            inp.remove()

class WindowNtupTask(ParallelProdWorkflow):

    # set previous_task which ParallelProdWorkflow uses to set the requirements
    previous_task = ("reco", RecoTask)

    def output(self):
        return self.local_target("windowntup_{}.root".format(self.branch))

    @law.decorator.notify
    @law.decorator.localize
    def run(self):
        inp = self.input()
        outp = self.output()

        cms_run_and_publish(self, "$CMSSW_BASE/src/RecoHGCal/GraphReco/test/windowNTuple_cfg.py", dict(
            inputFiles=[inp["reco"]["reco"].uri()],
            outputFile=outp.uri(),
        ))

        # remove RECO input after completion of the WindowNtupTask step
        for inp in law.util.flatten(inp["reco"]):
            inp.remove()
            
            