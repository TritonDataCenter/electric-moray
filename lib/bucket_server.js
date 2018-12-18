/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var artedi = require('artedi');
var fast = require('fast');
var fs = require('fs');
var kang = require('kang');
var net = require('net');
var os = require('os');
var restify = require('restify');
var uuid = require('node-uuid');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

var moray_client = require('./moray_client');
var dtrace = require('./dtrace');
var errors = require('./errors');
var schema = require('./schema');
var ring = require('./ring');
var data_placement = require('./data_placement');

var InvocationError = errors.InvocationError;
var ReadOnlyError = errors.ReadOnlyError;

var ALLOWED_BATCH_OPS = [
    'put',
    'delete'
];
var READ_ONLY = 'ro';
var KANG_VERSION = '1.2.0';

var B_ARGS_SCHEMA = [
    { name: 'requests', type: 'array' },
    { name: 'options', type: 'object' }
];

var CB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

var DB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

var DO_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'bucket_id', type: 'string' },
    { name: 'name', type: 'string' }
];

var DM_ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'filter', type: 'string' },
    { name: 'options', type: 'object' }
];

var GB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

var GO_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'bucket_id', type: 'string' },
    { name: 'name', type: 'string' }
];

var GT_ARGS_SCHEMA = [
    { name: 'options', type: 'object' }
];

var PO_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'bucket_id', type: 'string' },
    { name: 'name', type: 'string' },
    { name: 'content_length', type: 'number' },
    { name: 'content_md5', type: 'string' },
    { name: 'content_type', type: 'string' },
    { name: 'headers', type: 'object' },
    { name: 'sharks', type: 'object' },
    { name: 'properties', type: 'object' }
];


function createServer(options, callback) {
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    var log = options.log;
    var opts = {
        log: options.log
    };

    // remove ready flag
    log.info('server.createServer: removing ready cookie on startup');
    try {
        fs.unlinkSync('/var/tmp/electric-moray-ready');
    } catch (e) {
        // ignore failures if file DNE
    }

    data_placement.createDataDirector({
        log: options.log
    }, function (err, dataDirector) {
        if (err) {
            throw new verror.VError(err, 'unable to instantiate data director');
        }

        opts.dataDirector = dataDirector;

        log.info('creating moray clients');
        moray_client.createBucketClient({
            pnodes: opts.dataDirector.getPnodes(),
            morayOptions: options.morayOptions,
            log: options.log
        }, function (cErr, clients) {
            if (cErr) {
                throw new verror.VError(cErr, 'unable to create moray clients');
            }

            opts.clients = clients;
            // opts.indexShards = options.ringCfg.indexShards;

            var labels = {
                datacenter: options.datacenter,
                server: options.server_uuid,
                zonename: os.hostname(),
                pid: process.pid
            };

            var collector = artedi.createCollector({
                labels: labels
            });

            var socket = net.createServer({ 'allowHalfOpen': true });
            var server = new fast.FastServer({
                collector: collector,
                log: log.child({ component: 'fast' }),
                server: socket
            });

            var methods = [
                { rpcmethod: 'getbucket', rpchandler: getBucket(opts) },
                { rpcmethod: 'createbucket', rpchandler: createBucket(opts) },
                { rpcmethod: 'deletebucket', rpchandler: delBucket(opts) },
                { rpcmethod: 'getobject', rpchandler: getObject(opts) },
                { rpcmethod: 'putobject', rpchandler: putObject(opts) },
                { rpcmethod: 'deleteobject', rpchandler: delObject(opts) }
            ];

            methods.forEach(function (rpc) {
                server.registerRpcMethod(rpc);
            });

            var kangOpts = {
                service_name: 'electric-moray',
                version: KANG_VERSION,
                uri_base: '/kang',
                ident: os.hostname + '/' + process.pid,
                list_types: server.kangListTypes.bind(server),
                list_objects: server.kangListObjects.bind(server),
                get: server.kangGetObject.bind(server),
                stats: server.kangStats.bind(server)
            };

            var monitorServer = restify.createServer({
                name: 'Monitor'
            });

            monitorServer.get('/kang/.*', kang.knRestifyHandler(kangOpts));

            monitorServer.get('/metrics',
                function getMetricsHandler(req, res, next) {
                    req.on('end', function () {
                        assert.ok(collector, 'collector');
                        collector.collect(artedi.FMT_PROM,
                            function (cerr, metrics) {
                                if (cerr) {
                                    next(new verror.VError(err));
                                    return;
                                }
                                res.setHeader('Content-Type',
                                    'text/plain; version 0.0.4');
                                res.send(metrics);
                        });
                        next();
                    });
                    req.resume();
            });

            monitorServer.listen(options.monitorPort, options.bindip,
                function () {
                    log.info('monitor server started on port %d',
                        options.monitorPort);
            });

            socket.on('listening', function () {
                log.info('moray listening on %d', options.port);
                callback(null, {
                    dataDirector: opts.dataDirector,
                    // ring: opts.ring,
                    clientList: Object.keys(opts.clients.map)
                });
            });

            socket.on('error', function (serr) {
                log.error(serr, 'server error');
            });

            socket.listen(options.port, options.bindip);
        });
    });
}

function invalidArgs(rpc, argv, types) {
    var route = rpc.methodName();
    var len = types.length;

    if (argv.length !== len) {
        rpc.fail(new InvocationError(
            '%s expects %d argument%s %d', route, len, len === 1 ? '' : 's', argv.length));
        return true;
    }

    for (var i = 0; i < len; i++) {
        var name = types[i].name;
        var type = types[i].type;
        var val = argv[i];

        // 'array' is not a primitive type in javascript, but certain
        // rpcs expect them. Since typeof ([]) === 'object', we need to
        // special case this check to account for these expectations.
        if (type === 'array') {
            if (!Array.isArray(val)) {
                rpc.fail(new InvocationError('%s expects "%s" (args[%d]) to ' +
                            'be of type array but received type %s instead',
                            route, name, i, typeof (val)));
                return true;
            }
            continue;
        }

        if (type === 'object' && val === null) {
            rpc.fail(new InvocationError('%s expects "%s" (args[%d]) to ' +
                        'be an object but received the value "null"', route,
                        name, i));
            return true;
        }

        if (typeof (argv[i]) !== types[i].type) {
            rpc.fail(new InvocationError('%s expects "%s" (args[%d]) to be ' +
                'of type %s but received type %s instead', route, name, i,
                type, typeof (val)));
            return true;
        }
    }

    return false;
}


function createBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _createBucket(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, CB_ARGS_SCHEMA)) {
            return;
        }

        var o = argv[0];
        var b = argv[1];

        var id = options.req_id || uuid.v1();

        // dtrace['createbucket-start'].fire(function () {
        //     return ([msgid, id, b, k, opts._value]);
        // });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: o,
            bucket: b
        }, 'createBucket: entered');

        options.dataDirector.getBucketLocation(o, b, function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;
            log.info('pnode: ' + pnode);
            var client = options.clients.map[pnode];

            log.info('client: ' + client);

            client.createBucket(o, b, vnode, function (pErr, meta) {
                log.debug({
                    err: pErr,
                    meta: meta
                }, 'createBucket: returned');

                // dtrace['createbucket-done'].fire(function () {
                //     return ([msgid]);
                // });

                if (pErr) {
                    rpc.fail(pErr);
                } else {
                    // Add shard information to the response.
                    meta._node = location;

                    rpc.write(meta);
                    rpc.end();
                }
            });
        });
    }

    return _createBucket;
}


function getBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _getBucket(rpc) {
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, GB_ARGS_SCHEMA)) {
            return;
        }

        var o = argv[0];
        var b = argv[1];

        var id = options.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: o,
            bucket: b
        }, 'getBucket: entered');

        options.dataDirector.getBucketLocation(o, b, function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;
            var client = options.clients.map[pnode];

            options.clients.map[pnode].getBucket(o, b, vnode,
                function (gErr, rbucket) {
                log.debug({
                    err: gErr,
                    bucket: rbucket
                }, 'getBucket: returned');


                /*
                 * serialize the deserialized bucket response. To make this
                 * faster, we could:
                 * 1) modify the moray client to make deserializing optional.
                 * 2) directly hook up the streams by modifying the underlying
                 * node-fast stream.
                 */
                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    rpc.write(rbucket);
                    rpc.end();
                }
            });
        });
    }

    return _getBucket;
}


function delBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _delBucket(rpc) {
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, DB_ARGS_SCHEMA)) {
            return;
        }

        var o = argv[0];
        var b = argv[1];

        var id = options.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: o,
            bucket: b
        }, 'deleteBucket: entered');

        options.dataDirector.getBucketLocation(o, b, function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;
            var client = options.clients.map[pnode];

            options.clients.map[pnode].deleteBucket(o, b, vnode,
                function (gErr, rbucket) {
                log.debug({
                    err: gErr,
                    bucket: rbucket
                }, 'deleteBucket: returned');


                /*
                 * serialize the deserialized bucket response. To make this
                 * faster, we could:
                 * 1) modify the moray client to make deserializing optional.
                 * 2) directly hook up the streams by modifying the underlying
                 * node-fast stream.
                 */
                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    // rpc.write(rbucket);
                    rpc.end();
                }
            });
        });
    }

    return _delBucket;
}



function putObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _putObject(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, PO_ARGS_SCHEMA)) {
            return;
        }

        var o = argv[0];
        var b = argv[1];
        var k = argv[2];
        var content_length = argv[3];
        var content_md5 = argv[4];
        var content_type = argv[5];
        var headers = argv[6];
        var sharks = argv[7];
        var props = argv[8];

        var id = options.req_id || uuid.v1();

        // dtrace['putobject-start'].fire(function () {
        //     return ([msgid, id, b, k, opts._value]);
        // });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: o,
            bucket: b,
            key: k
        }, 'putObject: entered');

        options.dataDirector.getObjectLocation(o, b, k, function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var vnode = location.vnode;
            var pnode = location.pnode;
            var client = options.clients.map[pnode];

            if (props.constructor === Object && Object.keys(props).length === 0) {
                props = null;
            }

            client.putObject(o, b, k, content_length, content_md5, content_type,
                headers, sharks, props, vnode, function (pErr, meta) {
                log.debug({
                    err: pErr,
                    meta: meta
                }, 'putObject: returned');

                // dtrace['putobject-done'].fire(function () {
                //     return ([msgid]);
                // });

                if (pErr) {
                    rpc.fail(pErr);
                } else {
                    // Add shard information to the response.
                    meta._node = location;

                    rpc.write(meta);
                    rpc.end();
                }
            });
        });
    }

    return _putObject;
}


function getObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _getObject(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, GO_ARGS_SCHEMA)) {
            return;
        }

        var o = argv[0];
        var b = argv[1];
        var k = argv[2];

        var id = options.req_id || uuid.v1();

        // dtrace['getobject-start'].fire(function () {
        //     return ([msgid, id, b, k]);
        // });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: o,
            bucket: b,
            key: k
        }, 'getObject: entered');

        options.dataDirector.getObjectLocation(o, b, k, function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var pnode = location.pnode;
            var vnode = location.vnode;
            var client = options.clients.map[pnode];

            client.getObject(o, b, k, vnode, function (gErr, obj) {
                log.debug({
                    err: gErr,
                    obj: obj
                }, 'getObject: returned');

                // MANTA-1400: set the vnode info for debugging purposes
                if (obj) {
                    obj._node = location;
                }

                log.debug({
                    obj: obj
                }, 'sanitized object');

                // dtrace['getobject-done'].fire(function () {
                //     return ([msgid, obj]);
                // });

                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    rpc.write(obj);
                    rpc.end();
                }
            });
        });
    }

    return _getObject;
}

function delObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _delObject(rpc) {
        var msgid = rpc.requestId();
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, GO_ARGS_SCHEMA)) {
            return;
        }

        var o = argv[0];
        var b = argv[1];
        var k = argv[2];

        var id = options.req_id || uuid.v1();

        // dtrace['getobject-start'].fire(function () {
        //     return ([msgid, id, b, k]);
        // });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            owner: o,
            bucket: b,
            key: k
        }, 'delObject: entered');

        options.dataDirector.getObjectLocation(o, b, k, function (err, location) {
            if (err) {
                rpc.fail(err);
                return;
            }

            var pnode = location.pnode;
            var vnode = location.vnode;
            var client = options.clients.map[pnode];

            client.deleteObject(o, b, k, vnode, function (gErr, obj) {
                log.debug({
                    err: gErr,
                    obj: obj
                }, 'delObject: returned');

                // MANTA-1400: set the vnode info for debugging purposes
                if (obj) {
                    obj._node = location;
                }

                log.debug({
                    obj: obj
                }, 'sanitized object');

                // dtrace['getobject-done'].fire(function () {
                //     return ([msgid, obj]);
                // });

                if (gErr) {
                    rpc.fail(gErr);
                } else {
                    rpc.end();
                }
            });
        });
    }

    return _delObject;
}

module.exports = {
    createServer: createServer
};
