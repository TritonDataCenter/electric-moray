/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var path = require('path');


///--- Exports

module.exports = {
    // return the dirname of the key, if dirname === '.' this means there was
    // no dir, then just return the key as is
    manta: function manta(key) {
        /* JSSTYLED */
        var ROOT_RE = /^\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\/stor$/;
        var tkey = ROOT_RE.test(key) ? key : path.dirname(key);
        if (tkey === '.') {
            return key;
        } else {
            return tkey;
        }
    },
    // same as the manta bucket, used for unit tests so they don't stomp all
    // over the actual manta bucket
    testmanta: function testmanta(key) {
        /* JSSTYLED */
        var ROOT_RE = /^\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\/stor$/;
        var tkey = ROOT_RE.test(key) ? key : path.dirname(key);
        if (tkey === '.') {
            return key;
        } else {
            return tkey;
        }
    }
};
