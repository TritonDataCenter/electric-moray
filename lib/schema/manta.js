/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * These functions take care of transforming keys for some of the buckets
 * used through electric-moray. The keys are transformed so that some keys
 * will become the same value and always live on the same Moray shard. For
 * example, the "manta" bucket would transform these two keys prior to
 * hashing:
 *
 *     /a3829ca2-0966-60ce-9dc3-e4e9060f0950/stor/mydir/myobj1
 *     /a3829ca2-0966-60ce-9dc3-e4e9060f0950/stor/mydir/myobj2
 *
 * Into the value:
 *
 *     /a3829ca2-0966-60ce-9dc3-e4e9060f0950/stor/mydir
 *
 * This ensures that all objects in a directory have their information
 * stored on the same Moray shard, which is necessary for performing
 * a findObjects() call on the bucket's "dirname" index successfully.
 */

var path = require('path');
var strsplit = require('strsplit');

/*
 * Return the dirname of the key. If dirname === '.', then there was no
 * dir, so we just return the key as is.
 */
function dirnameOrKey(key) {
    /* JSSTYLED */
    var ROOT_RE = /^\/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\/stor$/;
    var tkey = ROOT_RE.test(key) ? key : path.dirname(key);
    if (tkey === '.') {
        return key;
    } else {
        return tkey;
    }
}


/*
 * "manta_uploads" has keys of the form <uuid>:<path>, and are hashed by
 * the dirname of the <path> component. Since a path could potentially
 * contain colons, we use strsplit to make sure we only split at the
 * first occurrence.
 */
function mpuDirnameOrKey(key) {
    var split = strsplit(key, ':', 2);
    if (split.length < 2) {
        return (key);
    }
    return (dirnameOrKey(split[1]));
}


///--- Exports

module.exports = {
    manta: function manta(key) {
        return (dirnameOrKey(key));
    },
    manta_uploads: function manta_uploads(key) {
        return (mpuDirnameOrKey(key));
    },
    // same as the manta bucket, used for unit tests so they don't stomp all
    // over the actual manta bucket
    testmanta: function testmanta(key) {
        return (dirnameOrKey(key));
    }
};
