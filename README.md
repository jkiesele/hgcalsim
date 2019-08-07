# HGCAL simulation using law


### Resources

- [law](https://law.readthedocs.io/en/latest)
- [luigi](https://luigi.readthedocs.io/en/stable)


### Setup

This repository has submodules, so you should clone it with

```shell
git clone --recursive https://github.com/CMS-HGCAL/hgcalsim.git
```

If you want to write files into the common hgcalsim output directory on EOS at `/eos/cms/store/cmst3/group/hgcal/CMG_studies/hgcalsim` (given that you have the necessary permissions), export

```shell
export HGC_STORE_EOS="/eos/cms/store/cmst3/group/hgcal/CMG_studies/hgcalsim"
```

or put the above line in your bashrc.

After cloning, run

```shell
source setup.sh
```

This will install CMSSW and a few python packages **once**. You should source the setup script everytime you start with a new session.


### Luigi scheduler

Task statuses and dependency trees can be visualized live using a central luigi scheduler. **This is optional** and no strict requirement to run tasks.

In order to let the tasks communicate with a central luigi scheduler, you should set

```shell
export HGC_SCHEDULER_HOST="..."
export HGC_SCHEDULER_PORT="..."
```

most probably in your bashrc file. **Otherwise**, you should add `--local-scheduler` to all `law run` commands.

You can also [setup a personal scheduler on OpenStack](https://github.com/CMS-HGCAL/hgcalsim/wiki#setting-up-a-luigi-scheduler-on-openstack).


### Storage on EOS

By default, most of the tasks write their output files to EOS. To prevent that (either because you don't have the necessary permissions on in case you want to test your code), add `--eos False` to the `law run` commands.


### Example commands

Re-compile CMSSW with 2 cores after making some updates to the code:

```shell
law run sw.CompileCMSSW --n-cores 2
```

Run GSD, RECO, and NTUP steps:

```shell
law run sim.NtupTask --n-events 2 --branch 0 --version dev
```

Run the above steps for 10 tasks on HTCondor:

```shell
law run sim.NtupTask --n-events 2 --n-tasks 10 --version dev --pilot --workflow htcondor
```
