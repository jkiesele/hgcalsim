# HGCAL simulation using law


### Resources

- [law](https://law.readthedocs.io/en/latest)
- [luigi](https://luigi.readthedocs.io/en/stable)


### Setup

This repository has submodules, so you should clone it with

```shell
git clone --recursive https://github.com/riga/hgcalsim.git
```

After cloning, run

```shell
source setup.sh
```

This will install CMSSW and a few python packages **once**. You should source the setup script everytime you start with a new session.

Also, in order to let the tasks communicate with a central luigi scheduler, you should set

```shell
export HGC_SCHEDULER_HOST="..."
export HGC_SCHEDULER_PORT="..."
```

most probably in your bashrc file. **Otherwise**, you should add `--local-scheduler` to all `law run` commands.


### Example commands

Re-compile CMSSW with 2 cores after making some updates to the code:

```shell
law run CompileCMSSW --n-cores 2
```

Run GSD, RECO, NTUP and conversion steps:

```shell
law run ConverterTask --n-events 2 --branch 0 --version dev
```

Run the above steps for 10 tasks on HTCondor:

```shell
law run ConverterTask --n-events 2 --n-tasks 10 --version dev1_converter --pilot --workflow htcondor
```

Merge the converted files into a configurable number of files (`--n-merged-files`):

```shell
law run MergeConvertedFiles --n-events 2 --n-tasks 10 --n-merged-files 1 --version dev1_converter \
    --ConverterTask-pilot --ConverterTask-workflow htcondor
```
