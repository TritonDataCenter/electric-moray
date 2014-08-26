/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var manta = require('./manta');



///--- API

function transformKey(bucket, key) {
    if (manta[bucket]) {
        return manta[bucket](key);
    } else {
        return key;
    }
}



///--- Exports

module.exports = {
    transformKey: transformKey
};
