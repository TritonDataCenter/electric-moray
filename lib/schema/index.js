// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var manta = require('./manta');



///--- API

function transformKey(bucket, key) {
    console.log(manta);
    console.log(manta.manta);
    if (manta[bucket]) {
        console.log('yunong, transforming');
        return manta[bucket](key);
    } else {
        console.log('xiao, not transforming');
        return key;
    }
}



///--- Exports

module.exports = {
    transformKey: transformKey
};
