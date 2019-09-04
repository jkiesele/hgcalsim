# coding: utf-8

"""
Tasks related to conversion and preprocessing of simulated events for use in GraphNNs.
"""


__all__ = ["ConverterTask", "MergeConvertedFiles"]


import os

import law
import luigi

from hgc.tasks.base import HTCondorWorkflow
from hgc.tasks.simulation import GeneratorParameters, ParallelProdWorkflow, NtupTask
from hgc.tasks.software import CompileConverter, CompileDeepJetCore
from hgc.util import hadd_task


luigi.namespace("gnn", scope=__name__)


class ConverterTask(ParallelProdWorkflow):

    previous_task = ("ntup", NtupTask)

    def workflow_requires(self):
        reqs = super(ConverterTask, self).workflow_requires()
        reqs["converter"] = CompileConverter.req(self)
        return reqs

    def requires(self):
        reqs = super(ConverterTask, self).requires()
        reqs["converter"] = CompileConverter.req(self)
        return reqs

    def output(self):
        return self.local_target("tuple_{}_n{}.root".format(self.branch, self.n_events))

    @law.decorator.notify
    def run(self):
        # determine the converter executable
        inp = self.input()
        converter = inp["converter"].path
        converter_dir = inp["converter"].parent

        # read the config template
        with converter_dir.child("config/config_template.txt").open("r") as f:
            template = f.read()

        # temporary output directory
        output_dir = law.LocalDirectoryTarget(is_tmp=True)
        output_dir.touch()

        # fill template variables
        with inp["ntup"].localize("r") as ntup_file:
            config = template.format(
                input_dir=ntup_file.parent.path,
                input_file=ntup_file.basename,
                output_dir=output_dir.path,
                hist_output_file="no_used.root",
                skim_output_prefix="output_file_",
            )

            # create a config file required by the converter
            config_file = law.LocalFileTarget(is_tmp=True)
            with config_file.open("w") as f:
                f.write(config)

            # run the converter
            env_script = converter_dir.child("env.sh").path
            cmd = "source {} '' && {} {}".format(env_script, converter, config_file.path)
            code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if code != 0:
                raise Exception("conversion failed")

        # determine the skim output file and
        output_basename = output_dir.glob("output_file_*")[0]
        self.output().copy_from_local(output_dir.child(output_basename))


class MergeConvertedFiles(GeneratorParameters, law.tasks.CascadeMerge):

    n_merged_files = luigi.IntParameter(description="number of files after merging")

    merge_factor = 10

    def cascade_workflow_requires(self):
        return ConverterTask.req(self, _prefer_cli=["workflow"])

    def trace_cascade_workflow_inputs(self, inputs):
        return self.n_tasks

    def cascade_requires(self, start_leaf, end_leaf):
        return [ConverterTask.req(self, branch=b) for b in range(start_leaf, end_leaf)]

    def cascade_output(self):
        return law.SiblingFileCollection(
            self.local_target("tuple_{}Of{}_n{}.root".format(
                i + 1, self.n_merged_files, self.n_events))
            for i in range(self.n_merged_files)
        )

    def merge(self, *args, **kwargs):
        return hadd_task(self, *args, **kwargs)


class CreateMLDataset(GeneratorParameters, law.LocalWorkflow, HTCondorWorkflow):

    n_merged_files = MergeConvertedFiles.n_merged_files
    data_structure = luigi.ChoiceParameter(default="hitlist",
        choices=["hitlist", "hitlist_layercluster"], description="name of the data structure to "
        "convert, prefixed by 'TrainData_', default: hitlist")

    def store_parts(self):
        return super(CreateMLDataset, self).store_parts() + (self.data_structure,)

    def create_branch_map(self):
        return {i: i for i in range(self.n_merged_files)}

    def workflow_requires(self):
        reqs = super(CreateMLDataset, self).workflow_requires()
        if not self.pilot:
            reqs["merged"] = MergeConvertedFiles.req(self, cascade_tree=-1, workflow="local",
                _prefer_cli=["version", "workflow"])
        else:
            reqs["conv"] = ConverterTask.req(self, _prefer_cli=["version", "workflow"])
        reqs["deepjetcore"] = CompileDeepJetCore.req(self)
        return reqs

    def requires(self):
        return {
            "merged": MergeConvertedFiles.req(self, cascade_tree=self.branch, branch=0,
                workflow="local", _prefer_cli=["version"]),
            "deepjetcore": CompileDeepJetCore.req(self),
        }

    def output(self):
        basename = os.path.splitext(self.input()["merged"].basename)[0]
        return law.SiblingFileCollection({
            "x": self.local_target(basename + ".x.0"),
            "y": self.local_target(basename + ".y.0"),
            "meta": self.local_target(basename + ".meta"),
            "dc": self.local_target(basename + ".dc"),
        })

    @law.decorator.notify
    def run(self):
        with self.input()["merged"].localize("r") as inp:
            # write the path of the input file to a temporary file
            samples_file = law.LocalFileTarget(is_tmp=True)
            samples_file.touch(content="{}\n".format(inp.path))

            # tmp dir for output files
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)

            # create the conversion command
            compile_task = self.requires()["deepjetcore"]
            cmd = """
                {} &&
                export HGCALML="$HGC_BASE/modules/HGCalML"
                export DEEPJETCORE_SUBPACKAGE="$HGCALML"
                export PYTHONPATH="$HGCALML/modules:$HGCALML/modules/datastructures:$PYTHONPATH"
                convertFromRoot.py -n 0 --noRelativePaths -c TrainData_{} -o "{}" -i "{}"
            """.format(compile_task.get_setup_cmd(), self.data_structure, tmp_dir.path,
                samples_file.path)

            # run the command
            code = law.util.interruptable_popen(cmd, env=compile_task.get_setup_env(), shell=True,
                executable="/bin/bash")[0]
            if code != 0:
                raise Exception("convertFromRoot.py failed")

        outp = self.output()
        outp["x"].copy_from_local(tmp_dir.child(outp["x"].basename))
        outp["y"].copy_from_local(tmp_dir.child(outp["y"].basename))
        outp["meta"].copy_from_local(tmp_dir.child(outp["meta"].basename))
        outp["dc"].copy_from_local(tmp_dir.child("dataCollection.dc"))
