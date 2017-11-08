#!/bin/sh

# Invoked as the stop method for the electric-moray service. This script
# disables registrar, which invokes registrars stop method to proactively remove
# electric-moray from zookeeper.

. /lib/svc/share/smf_include.sh

if [[ -z $(which svcs) ]]; then
    echo "Unable to find svcs command"
    exit ${SMF_EXIT_ERR_FATAL}
fi

REGISTRAR_FMRI=$(svcs | grep registrar | awk '{ print $3 }')

if [[ -z ${REGISTRAR_FMR} ]]; then
    echo "Unable to determine registrar FMRI. Is registrar running?"
    exit ${SMF_EXIT_ERR_FATAL}
fi

if [[ -z $(which svcadm) ]]; then
    echo "Cannot find svcadm in PATH"
    exit ${SMF_EXIT_ERR_FATAL}
fi

svcadm disable ${REGISTRAR_FMRI}

# Wait for cueball to notice that registrar has left DNS.
sleep 5

# Get PIDs of all the electric-moray processes.
PIDS=$(svcs -H -o FMRI -p electric-moray | tail -n +2 | awk '{ print $2 }')

if [[ -z ${PIDS} ]]; then
    echo "No electric-moray processes found"
    exit ${SMF_EXIT_ERR_FATAL}
fi

# Each electric-moray will catch the SIGTERM signal and enter the quiesce
# state, in which it will wait for pending requests on all of it's fast
# connections to finish.
kill -SIGTERM ${PIDS}

exit ${SMF_EXIT_OK}
