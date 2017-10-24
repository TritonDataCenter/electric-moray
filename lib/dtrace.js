/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var dtrace = require('dtrace-provider');



///--- Globals

var DTraceProvider = dtrace.DTraceProvider;

var PROBES = {
    // msgid, req_id, bucket, key, value
    'putobject-start': ['int', 'char *', 'char *', 'char *', 'char *'],

    // msgid
    'putobject-done': ['int'],

    // msgid, req_id
    'batch-start': ['int', 'char *'],

    // msgid
    'batch-done': ['int'],

    // msgid, req_id, bucket, key
    'getobject-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, value
    'getobject-done': ['int', 'json'],

    // msgid, req_id, bucket, key
    'delobject-start': ['int', 'char *', 'char *', 'char *'],

    // msgid
    'delobject-done': ['int'],

    // msgid, req_id, bucket, filter
    'findobjects-start': ['int', 'char *', 'char *', 'char *'],

    // msgid, key, id, etag, value
    'findobjects-record': ['int', 'char *', 'int', 'char *', 'char *'],

    // msgid, num_records
    'findobjects-done': ['int', 'int']
};
var PROVIDER;



///--- API

module.exports = function exportStaticProvider() {
    if (!PROVIDER) {
        PROVIDER = dtrace.createDTraceProvider('electric-moray');

        PROVIDER._fast_probes = {};

        Object.keys(PROBES).forEach(function (p) {
            var args = PROBES[p].splice(0);
            args.unshift(p);

            var probe = PROVIDER.addProbe.apply(PROVIDER, args);
            PROVIDER._fast_probes[p] = probe;
        });

        PROVIDER.enable();
    }

    return (PROVIDER);
}();
