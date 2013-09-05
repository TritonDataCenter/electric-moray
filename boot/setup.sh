#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-

# Boostraps the consistent hash ring for electric-moray.

set -o xtrace
set -o errexit
set -o pipefail

SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
SVC_ROOT=/opt/smartdc/electric-moray

source ${DIR}/scripts/util.sh
source ${DIR}/scripts/services.sh

export PATH=$SVC_ROOT/bin:$SVC_ROOT/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

RING_PREFIX=/opt/smartdc/electric-moray/etc
SERIALIZED_RING=$RING_PREFIX/ring.json
FASH=/opt/smartdc/electric-moray/node_modules/.bin/fash
LEVELDB_DIR_PARENT=/electric-moray/chash
LEVELDB_DIR=$LEVELDB_DIR_PARENT/leveldb-
ELECTRIC_MORAY_INSTANCES=1
ZONE_UUID=$(/usr/bin/zonename)
ZFS_PARENT_DATASET=zones/$ZONE_UUID/data
ZFS_DATASET=$ZFS_PARENT_DATASET/electric-moray

function manta_setup_electric_moray_instances {
    local size=`json -f ${METADATA} SIZE`
    if [ "$size" = "lab" ] || [ "$size" = "production" ]
    then
        ELECTRIC_MORAY_INSTANCES=4
    fi

    if [ "$size" = "lab" ]
    then
        cp $RING_PREFIX/lab.ring.json $SERIALIZED_RING
    fi

    if [ "$size" = "production" ]
    then
        cp $RING_PREFIX/prod.ring.json $SERIALIZED_RING
    fi

    if [ "$size" = "coal" ]
    then
        cp $RING_PREFIX/coal.ring.json $SERIALIZED_RING
    fi
}

function manta_setup_leveldb_hash_ring {
    # create the dataset
    zfs create -o canmount=noauto $ZFS_DATASET
    [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
    # create the mountpoint dir
    mkdir -p $LEVELDB_DIR_PARENT
    [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
    # set the mountpoint
    zfs set mountpoint=$LEVELDB_DIR_PARENT $ZFS_DATASET
    [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
    # mount the dataset
    zfs mount $ZFS_DATASET
    [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
    # build the list of leveldb locations
    local leveldb_dirs
    for (( i=1; i<=$ELECTRIC_MORAY_INSTANCES; i++ ))
    do
        leveldb_dirs[$i]=$LEVELDB_DIR$(expr 2020 + $i)
    done

    # try and load the topology from disk, if the load fails, we should error
    # out since we expect the topology to be there in the configure script
    for i in "${leveldb_dirs[@]}"
    do
        mkdir -p $i
        [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
        $FASH deserialize_ring -f $SERIALIZED_RING -l $i
        [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
    done
    ZFS_SNAPSHOT=$ZFS_DATASET@$(date +%s)000
    zfs snapshot $ZFS_SNAPSHOT
    [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
}

# Mainline

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/electric-moray"

manta_common_setup "electric-moray" 0

echo "Setting up leveldb"
manta_setup_electric_moray_instances
manta_setup_leveldb_hash_ring

exit 0

