// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var path = require('path');


///--- Exports

module.exports = {
    // return the dirname of the key, if dirname === '.' this means there was
    // no dir, then just return the key as is
    manta: function(key) {
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
