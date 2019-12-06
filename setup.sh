#!/usr/bin/env bash

action() {
    local origin="$( pwd )"

    # determine the directory of this file
    if [ ! -z "$ZSH_VERSION" ]; then
        local this_file="${(%):-%x}"
    else
        local this_file="${BASH_SOURCE[0]}"
    fi
    local this_dir="$( dirname "$this_file" )"

    # source the user setup file when existing
    [ -f "$this_dir/setup_user.sh" ] && source "$this_dir/setup_user.sh" ""

    # init and update the law submodule
    if [ ! -d "$this_dir/modules/law/law" ]; then
        (\
            cd "$this_dir" && \
            git submodule init modules/law && \
            git submodule update modules/law
        )
    else
        (\
            cd "$this_dir/modules/law" && \
            git checkout master && \
            git pull
        )
    fi


    #
    # global variables
    #

    # base directory
    export HGC_BASE="$( cd "$this_dir" && pwd )"

    # check if we're on lxplus
    if [[ "$( hostname )" = lxplus*.cern.ch ]]; then
        export HGC_ON_LXPLUS="1"
    else
        export HGC_ON_LXPLUS="0"
    fi

    # default grid user
    if [ -z "$HGC_GRID_USER" ]; then
        if [ "$HGC_ON_LXPLUS" = "1" ]; then
            export HGC_GRID_USER="$( whoami )"
            echo "NOTE: lxplus detected, setting HGC_GRID_USER to $HGC_GRID_USER"
        else
            2>&1 echo "please set HGC_GRID_USER to your grid user name and try again"
            return "1"
        fi
    fi
    export HGC_GRID_USER_FIRST_CHAR="${HGC_GRID_USER:0:1}"

    # other defaults
    [ -z "$HGC_DATA" ] && export HGC_DATA="$HGC_BASE/.data"
    [ -z "$HGC_SOFTWARE" ] && export HGC_SOFTWARE="$HGC_DATA/software/$( whoami )"
    [ -z "$HGC_STORE" ] && export HGC_STORE="$HGC_BASE/store"
    [ -z "$HGC_STORE_EOS_USER" ] && export HGC_STORE_EOS_USER="/eos/cms/store/cmst3/group/hgcal/CMG_studies/$HGC_GRID_USER/hgcalsim"
    [ -z "$HGC_STORE_EOS" ] && export HGC_STORE_EOS="$HGC_STORE_EOS_USER"
    [ -z "$HGC_CONDA_DIR" ] && export HGC_CONDA_DIR="/afs/cern.ch/work/j/jkiesele/public/conda_env/miniconda3"

    # store the location of the default gfal2 python bindings
    local gfal2_bindings_file="$( python -c "import gfal2; print(gfal2.__file__)" &> /dev/null )"
    [ "$?" != "0" ] && gfal2_bindings_file=""


    #
    # helper functions
    #

    hgc_install_pip() {
        pip install --ignore-installed --no-cache-dir --prefix "$HGC_SOFTWARE" "$@"
    }
    [ -z "$ZSH_VERSION" ] && export -f hgc_install_pip

    hgc_add_py() {
        [ ! -z "$1" ] && export PYTHONPATH="$1:$PYTHONPATH"
    }
    [ -z "$ZSH_VERSION" ] && export -f hgc_add_py

    hgc_add_bin() {
        [ ! -z "$1" ] && export PATH="$1:$PATH"
    }
    [ -z "$ZSH_VERSION" ] && export -f hgc_add_bin

    hgc_cmssw_base() {
        if [ -z "$CMSSW_VERSION" ]; then
            2>&1 echo "CMSSW_VERSION must be set for hgc_cmssw_path"
            return "1"
        fi
        echo "$HGC_BASE/cmssw/$( whoami )/$CMSSW_VERSION"
    }
    [ -z "$ZSH_VERSION" ] && export -f hgc_cmssw_base


    #
    # CMSSW setup
    # (hardcoded for the moment)
    #

    # default CMSSW version
    [ -z "$HGC_CMSSW_VERSION" ] && export HGC_CMSSW_VERSION="CMSSW_11_0_0_pre9"

    export SCRAM_ARCH="slc7_amd64_gcc820"
    export CMSSW_VERSION="$HGC_CMSSW_VERSION"
    export CMSSW_BASE="$( hgc_cmssw_base )"

    if [ ! -d "$CMSSW_BASE" ]; then
        echo "setting up $CMSSW_VERSION with $SCRAM_ARCH in $CMSSW_BASE"

        source "/cvmfs/cms.cern.ch/cmsset_default.sh" ""
        mkdir -p "$( dirname "$CMSSW_BASE" )"
        cd "$( dirname "$CMSSW_BASE" )"
        scramv1 project CMSSW "$CMSSW_VERSION" || return "$?"
        cd "$CMSSW_VERSION/src"
        eval `scramv1 runtime -sh` || return "$?"
        scram b || return "$?"

        # custom packages
        git cms-init

        git cms-addpkg IOMC/ParticleGuns
        git cms-addpkg SimCalorimetry/Configuration
        git cms-addpkg SimDataFormats/CaloAnalysis
        git cms-addpkg SimGeneral/CaloAnalysis
        git cms-addpkg SimGeneral/MixingModule
        git cms-checkout-topic riga:hgctruth

        git clone https://github.com/riga/reco-prodtools.git reco_prodtools
        ( cd reco_prodtools; git checkout hgctruth )

        git clone https://github.com/riga/reco-ntuples.git RecoNtuples
        ( cd RecoNtuples; git checkout hgctruth )

        # compile
        scram b -j
        if [ "$?" != "0" ]; then
            2>&1 echo "cmssw compilation failed"
            return "1"
        fi

        # create the prodtools base templates once
        cd reco_prodtools/templates/python
        ./produceSkeletons_D41_NoSmear_PU_AVE_200_BX_25ns.sh || return "$?"
        cd "$CMSSW_BASE/src"
        scram b python
        cd "$origin"
    else
        cd "$CMSSW_BASE/src"
        eval `scramv1 runtime -sh`
        cd "$origin"
    fi


    #
    # minimal software stack
    #

    # certificate proxy handling
    local proxy_base="x509up_u$( id -u )"
    if [ "$HGC_ON_HTCONDOR" = "1" ] && ls ${proxy_base}* &> /dev/null; then
        export X509_USER_CERT="/tmp/$proxy_base"
        cp "$( ls ${proxy_base}* | head -n 1 )" "$X509_USER_CERT"
        voms-proxy-info
    fi

    # variables for external software
    export GLOBUS_THREAD_MODEL="none"
    export PYTHONWARNINGS="ignore"
    export HGC_PYTHONPATH_ORIG="$PYTHONPATH"
    export HGC_GFAL_PLUGIN_DIR_ORIG="$GFAL_PLUGIN_DIR"
    export HGC_GFAL_PLUGIN_DIR="$HGC_SOFTWARE/gfal_plugins"

    # ammend software paths
    hgc_add_bin "$HGC_SOFTWARE/bin"
    hgc_add_py "$HGC_SOFTWARE/lib/python2.7/site-packages"
    hgc_add_py "$HGC_BASE/plotlib"

    # software that is used in this project
    hgc_install_software() {
        local origin="$( pwd )"
        local mode="$1"

        if [ -d "$HGC_SOFTWARE" ]; then
            if [ "$mode" = "force" ]; then
                echo "remove software in $HGC_SOFTWARE"
                rm -rf "$HGC_SOFTWARE"
            else
                if [ "$mode" != "silent" ]; then
                    echo "software already installed in $HGC_SOFTWARE"
                fi
                return "0"
            fi
        fi

        echo "installing software stack in $HGC_SOFTWARE"
        mkdir -p "$HGC_SOFTWARE"

        hgc_install_pip wget
        hgc_install_pip python-telegram-bot
        hgc_install_pip slackclient
        hgc_install_pip luigi

        # setup gfal, setup patched plugins, remove the http plugin
        if [ ! -z "$gfal2_bindings_file" ]; then
            ln -s "$gfal2_bindings_file" "$HGC_SOFTWARE/lib/python2.7/site-packages"
            export GFAL_PLUGIN_DIR="$HGC_GFAL_PLUGIN_DIR_ORIG"
            source "$(law location)/contrib/cms/scripts/setup_gfal_plugins.sh" "$HGC_GFAL_PLUGIN_DIR"
            unlink "$HGC_GFAL_PLUGIN_DIR/libgfal_plugin_http.so"
        else
            echo "skip setting up gfal2 plugins"
        fi
    }
    [ -z "$ZSH_VERSION" ] && export -f hgc_install_software
    hgc_install_software silent

    # update the GFAL_PLUGIN_DIR
    export GFAL_PLUGIN_DIR="$HGC_GFAL_PLUGIN_DIR"

    # add _this_ repo to the python path
    hgc_add_py "$HGC_BASE"

    # software from modules
    hgc_add_py "$HGC_BASE/modules/law"
    hgc_add_bin "$HGC_BASE/modules/law/bin"
    hgc_add_py "$HGC_BASE/modules/scinum"
    hgc_add_py "$HGC_BASE/modules/plotlib"


    #
    # law setup
    #

    export LAW_HOME="$HGC_BASE/.law"
    export LAW_CONFIG_FILE="$HGC_BASE/law.cfg"

    # configs that depend on the run location
    if [ "$HGC_ON_HTCONDOR" = "1" ] || [ "$HGC_ON_GRID" = "1" ]; then
        export HGC_LOCAL_CACHE="$LAW_JOB_HOME/cache"
        export HGC_LUIGI_WORKER_KEEP_ALIVE="False"
        export HGC_LUIGI_WORKER_FORCE_MULTIPROCESSING="True"
    else
        export HGC_LOCAL_CACHE="$HGC_DATA/cache"
        export HGC_LUIGI_WORKER_KEEP_ALIVE="False"
        export HGC_LUIGI_WORKER_FORCE_MULTIPROCESSING="True"
    fi

    if [ -z "$HGC_SCHEDULER_HOST" ]; then
        2>&1 echo "NOTE: HGC_SCHEDULER_HOST is not set, use '--local-scheduler' in your tasks!"
        export HGC_SCHEDULER_HOST=""
    fi
    export HGC_SCHEDULER_PORT="80"
    export HGC_LOCAL_SCHEDULER="False"

    # source law's bash completion scipt
    source "$( law completion )" ""

    # rerun the task indexing
    law index --verbose
}
action "$@"
