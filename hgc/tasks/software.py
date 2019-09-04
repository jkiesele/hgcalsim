# coding: utf-8

"""
Tasks related to software compilation.
"""


__all__ = ["CompileCMSSW", "CompileConverter", "DeepJetCore"]


import os

import law
import luigi

from hgc.tasks.base import Task


luigi.namespace("sw", scope=__name__)


class CompileCMSSW(Task, law.tasks.RunOnceTask):

    n_cores = luigi.IntParameter(default=1, significant=False, description="the number of cores to "
        "use for compiling cmssw")
    clean = luigi.BoolParameter(default=False, description="run 'scram b clean' before compiling")

    eos = None
    version = None

    @law.decorator.notify
    def run(self):
        # create the compilation command
        cmd = "scram b -j {}".format(self.n_cores)

        # prepend the cleanup command when clean is set
        if self.clean:
            cmd = "scram b clean; " + cmd

        # determine the directory in which to run
        cwd = os.path.expandvars("$CMSSW_BASE/src")

        # run the command
        code = law.util.interruptable_popen(cmd, cwd=cwd, shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception("CMSSW compilation failed")


class CompileConverter(Task):

    eos = None
    version = None

    def output(self):
        return law.LocalFileTarget("$HGC_BASE/modules/hgcal-rechit-input-dat-gen/analyser")

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        # create the compilation command
        cmd = "source env.sh '' && make clean && make"

        # determine the directory in which to run
        cwd = self.output().parent.path

        # run the command
        code = law.util.interruptable_popen(cmd, cwd=cwd, shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception("converter compilation failed")


class CompileDeepJetCore(Task):

    n_cores = luigi.IntParameter(default=1, significant=False, description="number of cores to use "
        "for compilation")
    clean = luigi.BoolParameter(default=False, description="run 'make clean' before compilation")

    eos = None
    version = None

    def output(self):
        return law.LocalFileTarget("$HGC_BASE/modules/DeepJetCore/compiled/classdict.so")

    def get_setup_cmd(self):
        # returns the command required to setup the conda and DeepJetCore env's
        conda_executable = "$HGC_CONDA_DIR/bin/conda"
        cmd = """
            eval "$( scram unsetenv -sh )" &&
            eval "$( {} shell.bash hook )" &&
            cd $HGC_BASE/modules/DeepJetCore &&
            source env.sh \
        """.format(conda_executable)
        return cmd

    def get_setup_env(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = env["HGC_PYTHONPATH_ORIG"]
        return env

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        # create the compilation command
        cmd = "{} && cd $HGC_BASE/modules/DeepJetCore/compiled".format(self.get_setup_cmd())
        if self.clean:
            cmd += " && make clean"
        cmd += " && make -j {}".format(self.n_cores)

        # run the command
        code = law.util.interruptable_popen(cmd, env=self.get_setup_env(), shell=True,
            executable="/bin/bash")[0]
        if code != 0:
            raise Exception("DeepJetCore compilation failed")
