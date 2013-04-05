// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

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
