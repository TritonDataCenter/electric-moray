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

FASH=/opt/smartdc/electric-moray/node_modules/.bin/fash
ELECTRIC_MORAY_INSTANCES=4
LEVELDB_DIR_PARENT=/electric-moray/chash
LEVELDB_DIR=$LEVELDB_DIR_PARENT/leveldb-
SAPI_URL=$(mdata-get SAPI_URL)
[[ -n $SAPI_URL ]] || fatal "no SAPI_URL found"
sleep 10 # wait 10 seconds for dns to setup, this is so lame but otherwise will resolve in dns resolution errors.
MANTA_APPLICATION=$(curl --connect-timeout 10 -sS -i -H accept:application/json \
    -H content-type:application/json --url $SAPI_URL/applications?name=manta)
[[ -n $MANTA_APPLICATION ]] || fatal "no MANTA_APPLICATION found"
HASH_RING_IMAGE=$(echo $MANTA_APPLICATION | json -Ha metadata.HASH_RING_IMAGE)
[[ -n $HASH_RING_IMAGE ]] || fatal "no HASH_RING_IMAGE found"
HASH_RING_FILE=/var/tmp/$(uuid -v4).tar.gz
export SDC_IMGADM_URL= $(echo $MANTA_APPLICATION | json -Ha metadata.IMGAPI_SERVICE)
[[ -n $SDC_IMGADM_URL ]] || fatal "no SDC_IMGADM_URL found"
ZONE_UUID=$(/usr/bin/zonename)
ZFS_PARENT_DATASET=zones/$ZONE_UUID/data
ZFS_DATASET=$ZFS_PARENT_DATASET/electric-moray

function manta_setup_leveldb_hash_ring {
    # get the hash ring image
    /opt/electric-moray/node_modules/.bin/sdc-imgadm get-file -o $HASH_RING_FILE
    local leveldb_ring_parent_dir=/var/tmp/$(uuid -v4)
    local leveldb_ring=$leveldb_ring_parent_dir/hash_ring
    tar -xzf $HASH_RING_FILE -C $leveldb_ring_parent_dir
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
    for dir in "${leveldb_dirs[@]}"
    do
        cp -R $leveldb_ring $dir
        [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
        # test with get_node on the newly created ring
        $FASH get_node -l $dir -b leveldb yunong
        [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
    done
    ZFS_SNAPSHOT=$ZFS_DATASET@$(date +%s)000
    zfs snapshot $ZFS_SNAPSHOT
    [[ $? -eq 0 ]] || fatal "unable to setup leveldb"
}

function manta_setup_electric_moray {
    #Build the list of ports.  That'll be used for everything else.
    local ports
    for (( i=1; i<=$ELECTRIC_MORAY_INSTANCES; i++ )); do
        ports[$i]=`expr 2020 + $i`
    done

    #To preserve whitespace in echo commands...
    IFS='%'

    #haproxy
    for port in "${ports[@]}"; do
        hainstances="$hainstances        server electric-moray-$port 127.0.0.1:$port check inter 10s slowstart 10s error-limit 3 on-error mark-down\n"
    done

    sed -e "s#@@ELECTRIC-MORAY_INSTANCES@@#$hainstances#g" \
        $SVC_ROOT/etc/haproxy.cfg.in > $SVC_ROOT/etc/haproxy.cfg || \
        fatal "could not process $src to $dest"

    svccfg import $SVC_ROOT/smf/manifests/haproxy.xml || \
        fatal "unable to import haproxy"
    svcadm enable "manta/haproxy" || fatal "unable to start haproxy"

    #electric-moray instances
    local electric_moray_xml_in=$SVC_ROOT/smf/manifests/electric-moray.xml.in
    for port in "${ports[@]}"; do
        local electric_moray_instance="electric-moray-$port"
        local electric_moray_xml_out=$SVC_ROOT/smf/manifests/electric-moray-$port.xml
        sed -e "s#@@ELECTRIC-MORAY_PORT@@#$port#g" \
            -e "s#@@ELECTRIC-MORAY_INSTANCE_NAME@@#$electric_moray_instance#g" \
            $electric_moray_xml_in  > $electric_moray_xml_out || \
            fatal "could not process $electric_moray_xml_in to $electric_moray_xml_out"

        svccfg import $electric_moray_xml_out || \
            fatal "unable to import $electric_moray_instance: $electric_moray_xml_out"
        svcadm enable "$electric_moray_instance" || \
            fatal "unable to start $electric_moray_instance"
    done

    unset IFS
}

function manta_setup_moray_rsyslogd {
    #rsyslog was already set up by common setup- this will overwrite the
    # config and restart since we want moray to log locally.
    local domain_name=$(json -f ${METADATA} domain_name)
    [[ $? -eq 0 ]] || fatal "Unable to domain name from metadata"

    mkdir -p /var/tmp/rsyslog/work
    chmod 777 /var/tmp/rsyslog/work

    cat > /etc/rsyslog.conf <<"HERE"
$MaxMessageSize 64k

$ModLoad immark
$ModLoad imsolaris
$ModLoad imudp


$template bunyan,"%msg:R,ERE,1,FIELD:(\{.*\})--end%\n"

*.err;kern.notice;auth.notice                   /dev/sysmsg
*.err;kern.debug;daemon.notice;mail.crit        /var/adm/messages

*.alert;kern.err;daemon.err                     operator
*.alert                                         root

*.emerg                                         *

mail.debug                                      /var/log/syslog

auth.info                                       /var/log/auth.log
mail.info                                       /var/log/postfix.log

$WorkDirectory /var/tmp/rsyslog/work
$ActionQueueType LinkedList
$ActionQueueFileName mantafwd
$ActionResumeRetryCount -1
$ActionQueueSaveOnShutdown on

HERE

        cat >> /etc/rsyslog.conf <<HERE

# Support node bunyan logs going to local0 and forwarding
# only as logs are already captured via SMF
# Uncomment the following line to get local logs via syslog
local0.* /var/log/electric-moray.log;bunyan
local0.* @@ops.$domain_name:10514

HERE

        cat >> /etc/rsyslog.conf <<"HERE"
$UDPServerAddress 127.0.0.1
$UDPServerRun 514

HERE

    svcadm restart system-log
    [[ $? -eq 0 ]] || fatal "Unable to restart rsyslog"

    #log pulling
    manta_add_logadm_entry "electric-moray" "/var/log" "exact"
}

# Mainline

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/electric-moray"

manta_common_setup "electric-moray" 0

echo "Setting up leveldb"
manta_setup_leveldb_hash_ring

echo "Setting up e-moray"
manta_setup_electric_moray
manta_setup_moray_rsyslogd

manta_common_setup_end

exit 0

