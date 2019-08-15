# coding: utf-8

"""
Custom base task classes.
"""


__all__ = ["Task", "HTCondorWorkflow"]


import os
import math
import getpass

import law
import luigi

law.contrib.load("htcondor", "slack", "tasks", "telegram", "root")


class Task(law.Task):
    """
    Custom base task.
    """

    version = luigi.Parameter(description="version of outputs to produce")
    notify = law.NotifyMultiParameter(parameters=[
        law.NotifyTelegramParameter(significant=False),
        law.NotifySlackParameter(significant=False),
    ])

    eos = luigi.BoolParameter(default=False, description="store local targets on EOS instead of in "
        "the local HGC_STORE directory, default: False")

    user = luigi.Parameter(default=getpass.getuser(), significant=False, description="the user "
        "executing the task, only for visual purposes on central luigi schedulers")

    exclude_params_req = {"notify"}
    exclude_params_branch = {"notify"}
    exclude_params_workflow = {"notify"}
    output_collection_cls = law.SiblingFileCollection
    workflow_run_decorators = [law.decorator.notify]
    message_cache_size = 20

    default_store = "$HGC_STORE"
    default_eos_store = "$HGC_STORE_EOS"

    def store_parts(self):
        parts = (self.task_family,)
        return parts

    def store_parts_opt(self):
        parts = tuple()
        if self.version is not None:
            parts += (self.version,)
        return parts

    def local_path(self, *path, **kwargs):
        # determine the path where to store targets
        default_store = self.default_eos_store if self.eos else self.default_store
        store_path = kwargs.get("store") or default_store

        # get fragements / parts from store_parts, store_parts_opt and the passed path
        # which are translated into directories (["a", "b", "c"] -> "a/b/c")
        parts = [str(p) for p in self.store_parts() + self.store_parts_opt() + path]

        # build the path and return it
        return os.path.join(os.path.expandvars(os.path.expanduser(store_path)), *parts)

    def local_target(self, *args, **kwargs):
        cls = law.LocalDirectoryTarget if kwargs.pop("dir", False) else law.LocalFileTarget
        return cls(self.local_path(*args, store=kwargs.pop("store", None)), **kwargs)


class HTCondorWorkflow(law.HTCondorWorkflow):
    """
    Custom htcondor workflow with good default configs for the CERN batch system.
    """

    poll_interval = luigi.FloatParameter(default=0.5, significant=False, description="time between "
        "status polls in minutes, default: 0.5")
    max_runtime = luigi.FloatParameter(default=24.0, significant=False, description="maximum "
        "runtime in hours")
    only_missing = luigi.BoolParameter(default=True, significant=False, description="skip tasks "
        "that are considered complete")
    cmst3 = luigi.BoolParameter(default=False, significant=False, description="use the CMS T3 "
        "HTCondor quota for jobs, default: False")

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(self.local_path(store="$HGC_STORE"))

    def htcondor_wrapper_file(self):
        return os.path.expandvars("$HGC_BASE/hgc/files/bash_wrapper.sh")

    def htcondor_bootstrap_file(self):
        return os.path.expandvars("$HGC_BASE/hgc/files/htcondor_bootstrap.sh")

    def htcondor_use_local_scheduler(self):
        return True

    def htcondor_job_config(self, config, job_num, branches):
        # render_data is rendered into all files sent with a job
        config.render_variables["hgc_base"] = os.getenv("HGC_BASE")
        # force to run on CC7, http://batchdocs.web.cern.ch/batchdocs/local/submit.html#os-choice
        config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))
        # copy the entire environment
        config.custom_content.append(("getenv", "true"))
        # fix for CERN htcondor batch: pass the true PATH variable as a render variable which is
        # used in the custom wapper file to set PATH
        config.render_variables["env_path"] = os.getenv("PATH")
        # the CERN htcondor setup requires a "log" config, but we can safely set it to /dev/null
        # if you are interested in the logs of the batch system itself, set a meaningful value here
        config.custom_content.append(("log", "/dev/null"))
        # set the maximum runtime
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        # CMS T3 group settings
        if self.cmst3:
            config.custom_content.append(("+AccountingGroup", "group_u_CMST3.all"))

        return config
