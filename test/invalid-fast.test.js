/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var util = require('util');
var VError = require('verror');

var helper = require('./helper');

var after = helper.after;
var before = helper.before;
var test = helper.test;

var LOG = helper.createLogger('electric-moray-invalid-fast');

var BAD_RPCS = [
    {
        method: 'fakerpc',
        args: [],
        errname: 'FastError',
        errmsg: 'unsupported RPC method: "fakerpc"'
    },
    {
        method: 'batch',
        args: [],
        errname: 'InvocationError',
        errmsg: 'batch expects 2 arguments'
    },
    {
        method: 'batch',
        args: [ {}, [] ],
        errname: 'InvocationError',
        errmsg: 'batch expects "requests" (args[0]) to be of type array but ' +
            'received type object instead'
    },
    {
        method: 'batch',
        args: [[ {} ], 0],
        errname: 'InvocationError',
        errmsg: 'batch expects "options" (args[1]) to be of type object but ' +
            'received type number instead'
    },
    {
        method: 'batch',
        args: [[ {} ], null],
        errname: 'InvocationError',
        errmsg: 'batch expects "options" (args[1]) to be an object but ' +
            'received the value "null"'
    },
    {
        method: 'batch',
        args: [[], {}],
        errname: 'InvocationError',
        errmsg: 'batch expects "requests" (args[0]) to be an array with at ' +
            'least one request but received an empty array'
    },
    {
        method: 'createBucket',
        args: [],
        errname: 'InvocationError',
        errmsg: 'createBucket expects 3 arguments'
    },
    {
        method: 'createBucket',
        args: [0, 1, 2],
        errname: 'InvocationError',
        errmsg: 'createBucket expects "bucket" (args[0]) to be of type ' +
            'string but received type number instead'
    },
    {
        method: 'createBucket',
        args: ['0', 1, 2],
        errname: 'InvocationError',
        errmsg: 'createBucket expects "config" (args[1]) to be of type ' +
            'object but received type number instead'
    },
    {
        method: 'createBucket',
        args: ['0', {}, 2],
        errname: 'InvocationError',
        errmsg: 'createBucket expects "options" (args[2]) to be of type ' +
            'object but received type number instead'
    },
    {
        method: 'delBucket',
        args: [],
        errname: 'InvocationError',
        errmsg: 'delBucket expects 2 arguments'
    },
    {
        method: 'delBucket',
        args: [0, 1],
        errname: 'InvocationError',
        errmsg: 'delBucket expects "bucket" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'delBucket',
        args: ['0', 1],
        errname: 'InvocationError',
        errmsg: 'delBucket expects "options" (args[1]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'delObject',
        args: [],
        errname: 'InvocationError',
        errmsg: 'delObject expects 3 arguments'
    },
    {
        method: 'delObject',
        args: [0, 1, 2],
        errname: 'InvocationError',
        errmsg: 'delObject expects "bucket" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'delObject',
        args: ['0', 1, 2],
        errname: 'InvocationError',
        errmsg: 'delObject expects "key" (args[1]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'delObject',
        args: ['0', '1', 2],
        errname: 'InvocationError',
        errmsg: 'delObject expects "options" (args[2]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'deleteMany',
        args: [],
        errname: 'InvocationError',
        errmsg: 'deleteMany expects 3 arguments'
    },
    {
        method: 'deleteMany',
        args: [0, 1, 2],
        errname: 'InvocationError',
        errmsg: 'deleteMany expects "bucket" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'deleteMany',
        args: ['0', 1, 2],
        errname: 'InvocationError',
        errmsg: 'deleteMany expects "filter" (args[1]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'deleteMany',
        args: ['0', '1', 2],
        errname: 'InvocationError',
        errmsg: 'deleteMany expects "options" (args[2]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'findObjects',
        args: [],
        errname: 'InvocationError',
        errmsg: 'findObjects expects 3 arguments'
    },
    {
        method: 'findObjects',
        args: [0, 1, 2],
        errname: 'InvocationError',
        errmsg: 'findObjects expects "bucket" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'findObjects',
        args: ['0', 1, 2],
        errname: 'InvocationError',
        errmsg: 'findObjects expects "filter" (args[1]) to be of type ' +
            'string but received type number instead'

    },
    {
        method: 'findObjects',
        args: ['0', '1', 2],
        errname: 'InvocationError',
        errmsg: 'findObjects expects "options" (args[2]) to be of type ' +
            'object but received type number instead'
    },
    {
        method: 'getBucket',
        args: [],
        errname: 'InvocationError',
        errmsg: 'getBucket expects 2 arguments'
    },
    {
        method: 'getBucket',
        args: [0, 1],
        errname: 'InvocationError',
        errmsg: 'getBucket expects "options" (args[0]) to be of type ' +
            'object but received type number instead'
    },
    {
        method: 'getBucket',
        args: [ {}, 1],
        errname: 'InvocationError',
        errmsg: 'getBucket expects "bucket" (args[1]) to be of type ' +
            'string but received type number instead'
    },
    {
        method: 'getObject',
        args: [],
        errname: 'InvocationError',
        errmsg: 'getObject expects 3 arguments'
    },
    {
        method: 'getObject',
        args: [0, 1, 2],
        errname: 'InvocationError',
        errmsg: 'getObject expects "bucket" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'getObject',
        args: ['0', 1, 2],
        errname: 'InvocationError',
        errmsg: 'getObject expects "key" (args[1]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'getObject',
        args: ['0', '1', 2],
        errname: 'InvocationError',
        errmsg: 'getObject expects "options" (args[2]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'getTokens',
        args: [],
        errname: 'InvocationError',
        errmsg: 'getTokens expects 1 argument'
    },
    {
        method: 'getTokens',
        args: [0],
        errname: 'InvocationError',
        errmsg: 'getTokens expects "options" (args[0]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'putObject',
        args: [],
        errname: 'InvocationError',
        errmsg: 'putObject expects 4 argument'
    },
    {
        method: 'putObject',
        args: [0, 1, 2, 3],
        errname: 'InvocationError',
        errmsg: 'putObject expects "bucket" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'putObject',
        args: ['0', 1, 2, 3],
        errname: 'InvocationError',
        errmsg: 'putObject expects "key" (args[1]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'putObject',
        args: ['0', '1', 2, 3],
        errname: 'InvocationError',
        errmsg: 'putObject expects "value" (args[2]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'putObject',
        args: ['0', '1', {}, 3],
        errname: 'InvocationError',
        errmsg: 'putObject expects "options" (args[3]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'sql',
        args: [],
        errname: 'InvocationError',
        errmsg: 'sql expects 3 arguments'
    },
    {
        method: 'sql',
        args: [0, 1, 2],
        errname: 'InvocationError',
        errmsg: 'sql expects "statement" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'sql',
        args: ['0', 1, 2],
        errname: 'InvocationError',
        errmsg: 'sql expects "values" (args[1]) to be of type array ' +
            'but received type number instead'
    },
    {
        method: 'sql',
        args: ['0', [], 2],
        errname: 'InvocationError',
        errmsg: 'sql expects "options" (args[2]) to be of type object ' +
            'but received type number instead'
    },
    {
        method: 'updateBucket',
        args: [],
        errname: 'InvocationError',
        errmsg: 'updateBucket expects 3 arguments'
    },
    {
        method: 'updateBucket',
        args: [0, 1, 2],
        errname: 'InvocationError',
        errmsg: 'updateBucket expects "name" (args[0]) to be of type string ' +
            'but received type number instead'
    },
    {
        method: 'updateBucket',
        args: ['0', 1, 2],
        errname: 'InvocationError',
        errmsg: 'updateBucket expects "config" (args[1]) to be of type ' +
            'object but received type number instead'
    },
    {
        method: 'updateBucket',
        args: ['0', {}, 2],
        errname: 'InvocationError',
        errmsg: 'updateBucket expects "options" (args[2]) to be of type ' +
            'object but received type number instead'
    },
    {
        method: 'updateObjects',
        args: [],
        errname: 'InvocationError',
        errmsg: 'updateObjects expects 4 arguments'
    },
    {
        method: 'updateObjects',
        args: [0, 1, 2, 3],
        errname: 'InvocationError',
        errmsg: 'updateObjects expects "bucket" (args[0]) to be of type ' +
            'string but received type number instead'
    },
    {
        method: 'updateObjects',
        args: ['0', 1, 2, 3],
        errname: 'InvocationError',
        errmsg: 'updateObjects expects "fields" (args[1]) to be of type ' +
            'object but received type number instead'
    },
    {
        method: 'updateObjects',
        args: ['0', {}, 2, 3],
        errname: 'InvocationError',
        errmsg: 'updateObjects expects "filter" (args[2]) to be of type ' +
            'string but received type number instead'
    },
    {
        method: 'updateObjects',
        args: ['0', {}, '2', 3],
        errname: 'InvocationError',
        errmsg: 'updateObjects expects "options" (args[3]) to be of type ' +
            'object but received type number instead'
    }
];

BAD_RPCS.forEach(function (rpc) {
    assert.string(rpc.method, 'method name');
    assert.array(rpc.args, 'argument array');
    assert.string(rpc.errname, 'error name');
    assert.string(rpc.errmsg, 'error message');

    test(rpc.errname + ':' + rpc.errmsg, function (t) {
        helper.makeFastRequest({
            log: LOG,
            call: {
                rpcmethod: rpc.method,
                rpcargs: rpc.args,
                maxObjectsToBuffer: 100
            }
        }, function (err, data, ndata) {
            t.ok(err, 'expected error');
            t.deepEqual([], data, 'expected no data');
            t.deepEqual(0, ndata, 'expected no results');

            if (err) {
                var cause = VError.findCauseByName(err, rpc.errname);
                t.ok(cause, 'expected a ' + rpc.errname);
                if ((cause && cause.message.indexOf(rpc.errmsg) !== -1) ||
                    (err.message.indexOf(rpc.errmsg) !== -1)) {
                    t.ok(true, 'correct error message');
                } else {
                    t.equal(err.message, rpc.errmsg, 'correct error message');
                }
            }
            t.end();
        });
    });
});
