<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# electric-moray

Node-based service that provides the same interface as
[Moray](https://github.com/joyent/moray), but which directs requests to one or
more Moray+Manatee shards based on hashing of the Moray key.

