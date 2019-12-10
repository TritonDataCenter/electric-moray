<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019 Joyent, Inc.
-->

# electric-moray

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

electric-moray is a Node-based service that provides the same interface as
[Moray](https://github.com/joyent/moray), but which directs requests to one or
more Moray+Manatee shards based on hashing of the Moray key.

## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/joyent/manta/blob/master/docs/mantav2.md) for
details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.


## Building and running

To run your own electric-moray from a copy of this repository, you'll want:

* a Manta deployment, which includes at least one metadata shard of Moray and
  Manatee,
* an electric-moray configuration file, and
* an electric-moray consistent hash ring configuration.

To point electric-moray at your existing deployment, you'll want to make sure
that your development environment can reach that deployment (e.g., has an
interface on the "manta" network) and that the Manta deployment's nameservers
are included in /etc/resolv.conf in your development environment.

The easiest way to obtain a working configuration file and hash ring
configuration is to copy them from one of the electric-moray zones in your
existing Manta deployment:

* configuration file: /opt/smartdc/electric-moray/etc/config.json.
* hash ring: /electric-moray/chash/leveldb-2021

In a single-server deployment (as is typically used for testing), you can use
`cp -a` in the global zone to copy these from the electric-moray zone to your
test zone.

Once you've got these pieces in place, install the dependencies:

    $ make

update your path:

    $ source env.sh

and run electric-moray with something like this:

    $ node ./main.js -f /path/to/config.json -r /path/to/hash/ring -p 2020 \
        2>&1 | bunyan

For example, if the configuration file and hash ring were copied to your
electric-moray workspace, you'd use:

    $ node ./main.js -f ./config.json -r ./leveldb-2021 -p 2020 2>&1 | bunyan


## Testing

First, make sure you're running a local copy of electric-moray as described
above.  Then, run the test suite:

    $ make test

This assumes that an electric-moray server is running on localhost port 2020.

## Logging

Each electric-moray process (`node main.js ...`) logs to stderr. That means
this log output goes to the SMF service's log file:

    svcs -L electric-moray

By default it logs at the bunyan "info" level. The level is set via the
`bunyan.level` key in the config file (see [the config file
template](./sapi_manifests/electric-moray/template)). As an override, one
can specify the `-v` option to `node main.js ...` to get "trace" level logging.

In a small Manta there will just be one electric-moray process. In a large
Manta there will be multiple (currently 4) processes in each electric-moray
zone.

To watch electric-moray logs use:

    tail -f `svcs -L electric-moray` | bunyan

To watch trace-level logs for an already running electric-moray:

    bunyan -p $PID

Logs are rotated and uploaded hourly via a cronjob. They are uploaded to
the local Manta at:

    /poseidon/stor/logs/electric-moray/YYYY/MM/DD/HH/$zonePrefix.$port.log
