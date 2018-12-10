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

// var CB_ARGS_SCHEMA = [
//     { name: 'bucket', type: 'string' },
//     { name: 'config', type: 'object' },
//     { name: 'options', type: 'object' }
// ];

var CB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

// var DB_ARGS_SCHEMA = [
//     { name: 'bucket', type: 'string' },
//     { name: 'options', type: 'object' }
// ];

var DB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' },
    { name: 'vnode', type: 'number' }
];

var DO_ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'key', type: 'string' },
    { name: 'options', type: 'object' }
];

var DM_ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'filter', type: 'string' },
    { name: 'options', type: 'object' }
];

var FO_ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'filter', type: 'string' },
    { name: 'options', type: 'object' }
];

// var GB_ARGS_SCHEMA = [
//     { name: 'options', type: 'object' },
//     { name: 'bucket', type: 'string' }
// ];

var GB_ARGS_SCHEMA = [
    { name: 'owner', type: 'string' },
    { name: 'name', type: 'string' }
];

// var GO_ARGS_SCHEMA = [
//     { name: 'bucket', type: 'string' },
//     { name: 'key', type: 'string' },
//     { name: 'options', type: 'object' }
// ];

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

// var PO_ARGS_SCHEMA = [
//     { name: 'bucket', type: 'string' },
//     { name: 'key', type: 'string' },
//     { name: 'value', type: 'object' },
//     { name: 'options', type: 'object' }
// ];

var SQL_ARGS_SCHEMA = [
    { name: 'statement', type: 'string' },
    { name: 'values', type: 'array' },
    { name: 'options', type: 'object' }
];

var UB_ARGS_SCHEMA = [
    { name: 'name', type: 'string' },
    { name: 'config', type: 'object' },
    { name: 'options', type: 'object' }
];

var UO_ARGS_SCHEMA = [
    { name: 'bucket', type: 'string' },
    { name: 'fields', type: 'object' },
    { name: 'filter', type: 'string' },
    { name: 'options', type: 'object' }
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
                // { rpcmethod: 'batch', rpchandler: batch(opts) },
                { rpcmethod: 'createBucket', rpchandler: createBucket(opts) },
                { rpcmethod: 'delBucket', rpchandler: delBucket(opts) },
                { rpcmethod: 'delObject', rpchandler: delObject(opts) },
                // { rpcmethod: 'deleteMany', rpchandler: deleteMany(opts) },
                // { rpcmethod: 'findObjects', rpchandler: findObjects(opts) },
                { rpcmethod: 'getBucket', rpchandler: getBucket(opts) },
                { rpcmethod: 'getObject', rpchandler: getObject(opts) },
                // { rpcmethod: 'getTokens', rpchandler: getTokens(opts) },
                { rpcmethod: 'putObject', rpchandler: putObject(opts) },
                // { rpcmethod: 'sql', rpchandler: sql(opts) },
                // { rpcmethod: 'updateBucket', rpchandler: updateBucket(opts) },
                // { rpcmethod: 'updateObjects', rpchandler: updateObjects(opts) }
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
            '%s expects %d argument%s', route, len, len === 1 ? '' : 's'));
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

/*
 * Returns a list of pnodes that are not safe for write operations. Parameters
 * are a bunyan logger for the child process that called this function, and an
 * options hash containing:
 *      "indexShards"       an array of index pnodes from our configuration file
 *      "clients"           a list of moray client tracking objects
 */
// function listReadOnlyPnodes(log, options) {
//     assert.object(log, 'log');
//     assert.object(options, 'options');
//     assert.arrayOfObject(options.indexShards, 'options.indexShards');
//     assert.object(options.clients, 'options.clients');

//     log.debug({
//         indexShards: options.indexShards,
//         clients: options.clients
//     }, 'listReadOnlyPnodes: entered');

//     var pnodes = Object.keys(options.clients.map);
//     var readOnlyPnodes = pnodes.filter(function (pnode) {
//         return (isReadOnlyPnode(log, options.indexShards, pnode));
//     });

//     return readOnlyPnodes;
// }

/*
 * Checks if a moray client is set to read-only mode. Parameters are as follows:
 *      "log"               a bunyan logger
 *      "indexShards"       an array of index pnodes from our configuration file
 *      "pnode"             a physical node name
 */
// function isReadOnlyPnode(log, indexShards, pnode) {
//     assert.object(log, 'log');
//     assert.arrayOfObject(indexShards, 'indexShards');
//     assert.string(pnode, 'pnode');

//     log.debug({
//         indexShards: indexShards,
//         pnode: pnode
//     }, 'isReadOnlyPnode: entered');

//     var readOnlyStatus = null;
//     for (var i = 0; i < indexShards.length; i++) {
//         var shard = indexShards[i];
//         assert.optionalBool(shard.readOnly, 'shard.readOnly');
//         assert.string(shard.host, 'shard.host');
//         var shard_url = 'tcp://' + shard.host + ':2020';
//         if (shard_url === pnode) {
//             /*
//              * A shard in indexShards may not have a 'readOnly' field, which
//              * would mean it is not set to read-only mode.
//              */
//             readOnlyStatus = !!shard.readOnly;
//             return readOnlyStatus;
//         }
//     }

//     /*
//      * If no shard's host field in indexShards matches the pnode passed to this
//      * function, we are in an inconsistent state, and it is safer to assume that
//      * the pnode is read-only than the alternative, which may result in writes
//      * in error.
//      */
//     log.warn({
//         indexShards: indexShards,
//         pnode: pnode
//     }, 'isReadOnlyPnode: pnode not found in indexShards');
//     return true;
// }


// function createBucket(options) {
//     assert.object(options, 'options');
//     assert.object(options.log, 'options.log');
//     assert.object(options.clients, 'options.clients');
//     assert.object(options.indexShards, 'options.indexShards');

//     function _createBucket(rpc) {
//         var argv = rpc.argv();

//         if (invalidArgs(rpc, argv, CB_ARGS_SCHEMA)) {
//             return;
//         }

//         var owner = argv[0];
//         var name = argv[1];
//         // var cfg = argv[1];
//         // var opts = argv[2];

//         var id = opts.req_id || uuid.v1();
//         var log = options.log.child({
//             req_id: id
//         });

//         log.debug({
//             owner: owner,
//             bucket: name
//         }, 'createBucket: entered');

//         // if (options.ring.ro_) {
//         //     log.debug({
//         //         bucket: name,
//         //         cfg: cfg,
//         //         opts: opts,
//         //         ro: options.ring.ro_
//         //     }, 'createBucket: failed shard is read only');
//         //     rpc.fail(new ReadOnlyError());
//         //     return;
//         // }
//         var err = [];
//         var done = 0;

//         var readOnlyPnodes = listReadOnlyPnodes(log, options);
//         if (readOnlyPnodes.length > 0) {
//             rpc.fail(new ReadOnlyError(readOnlyPnodes.join(', ')));
//             return;
//         }

//         options.clients.array.forEach(function (client) {
//             client.createBucket(name, cfg, opts, function (err2) {
//                 log.debug({
//                     err: err2,
//                     client: client.host
//                 }, 'createBucket: returned');

//                 if (err2) {
//                     err.push(err2);
//                 }

//                 if (++done === options.clients.array.length) {
//                     var multiError = err[0]
//                         ? new verror.MultiError(err) : null;
//                     log.debug({
//                         err: multiError
//                     }, 'createBucket: finished all shards');

//                     if (multiError) {
//                         rpc.fail(multiError);
//                     } else {
//                         rpc.end();
//                     }
//                 }
//             });
//         });
//     }

//     return _createBucket;
// }

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
            // if (node.data && node.data === READ_ONLY) {
            //     log.debug({
            //         bucket: b,
            //         key: k,
            //         value: v,
            //         opts: opts,
            //         node: node
            //     }, 'createBucket: failed vnode is read only');
            //     rpc.fail(new ReadOnlyError());
            //     return;
            // }
            var vnode = location.vnode;
            var pnode = location.pnode;
            log.info('pnode: ' + pnode);
            var client = options.clients.map[pnode];

            log.info('client: ' + client);
            // if (isReadOnlyPnode(log, options.indexShards, pnode)) {
            //     rpc.fail(new ReadOnlyError(pnode));
            //     return;
            // }

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
                    // rbucket.index = JSON.stringify(rbucket.index);
                    // rbucket.pre.forEach(function (fn, index) {
                    //     rbucket.pre[index] = fn.toString();
                    // });
                    // rbucket.pre = JSON.stringify(rbucket.pre);
                    // rbucket.post.forEach(function (fn, index) {
                    //     rbucket.post[index] = fn.toString();
                    // });
                    // rbucket.post = JSON.stringify(rbucket.post);
                    // rbucket.options = JSON.stringify(rbucket.options);
                    // rbucket.mtime = JSON.stringify(rbucket.mtime.toString());
                    // if (rbucket.hasOwnProperty('reindex_active')) {
                    //     rbucket.reindex_active = JSON.stringify(
                    //         rbucket.reindex_active);
                    // }

                    rpc.write(rbucket);
                    rpc.end();
                }
            });
        });
    }

    return _getBucket;
}

function updateBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _updateBucket(rpc) {
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, UB_ARGS_SCHEMA)) {
            return;
        }

        var name = argv[0];
        var cfg = argv[1];
        var opts = argv[2];

        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: name,
            cfg: cfg,
            opts: opts
        }, 'updateBucket: entered');

        if (options.ring.ro_) {
            log.debug({
                bucket: name,
                cfg: cfg,
                opts: opts,
                ro: options.ring.ro_
            }, 'updateBucket: failed shard is read only');
            rpc.fail(new ReadOnlyError());
            return;
        }

        var err = [];
        var done = 0;

        var readOnlyPnodes = listReadOnlyPnodes(log, options);
        if (readOnlyPnodes.length > 0) {
            rpc.fail(new ReadOnlyError(readOnlyPnodes.join(', ')));
            return;
        }

        options.clients.array.forEach(function (client) {
            client.updateBucket(name, cfg, opts, function (err2) {
                log.debug({
                    err: err2
                }, 'updateBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.clients.array.length) {
                    var multiError = err[0] ?
                        new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'updateBucket: finished all shards');

                    if (multiError) {
                        rpc.fail(multiError);
                    } else {
                        rpc.end();
                    }
                }
            });
        });
    }

    return _updateBucket;
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

        var name = argv[0];
        var opts = argv[1];

        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: name,
            opts: opts
        }, 'delBucket: entered');

        if (options.ring.ro_) {
            log.debug({
                bucket: name,
                opts: opts,
                ro: options.ring.ro_
            }, 'deleteBucket: failed shard is read only');
            rpc.fail(new ReadOnlyError());
            return;
        }

        var err = [];
        var done = 0;

        var readOnlyPnodes = listReadOnlyPnodes(log, options);
        if (readOnlyPnodes.length > 0) {
            rpc.fail(new ReadOnlyError(readOnlyPnodes.join(', ')));
            return;
        }

        options.clients.array.forEach(function (client) {
            client.delBucket(name, opts, function (err2) {
                log.debug({
                    err: err2
                }, 'delBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.clients.array.length) {
                    var multiError = err[0] ?
                        new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'delBucket: finished all shards');

                    if (multiError) {
                        rpc.fail(multiError);
                    } else {
                        rpc.end();
                    }
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

        var o = argv[0];
        var b = argv[1];
        var k = argv[2];
        var content_length = argv[3];
        var content_md5 = argv[4];
        var content_type = argv[5];
        var headers = argv[6];
        var sharks = argv[7];
        var props = argv[8];

        var id = opts.req_id || uuid.v1();

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
            // if (node.data && node.data === READ_ONLY) {
            //     log.debug({
            //         bucket: b,
            //         key: k,
            //         value: v,
            //         opts: opts,
            //         node: node
            //     }, 'putObject: failed vnode is read only');
            //     rpc.fail(new ReadOnlyError());
            //     return;
            // }
            var vnode = location.vnode;
            var pnode = node.pnode;
            var client = options.clients.map[pnode];

            // if (isReadOnlyPnode(log, options.indexShards, pnode)) {
            //     rpc.fail(new ReadOnlyError(pnode));
            //     return;
            // }

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

        var id = opts.req_id || uuid.v1();

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

                // delete the vnode from the value, as the vnode is only used
                // internally
                // if (obj && obj.value) {
                //     delete obj.value.vnode;
                // }
                // if (obj && obj._value) {
                //     delete obj._value.vnode;
                // }

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

        if (invalidArgs(rpc, argv, DO_ARGS_SCHEMA)) {
            return;
        }

        var b = argv[0];
        var k = argv[1];
        var opts = argv[2];

        var id = opts.req_id || uuid.v1();

        dtrace['delobject-start'].fire(function () {
            return ([msgid, id, b, k]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'delObject: entered');

        options.ring.getNode(b, k, function (err, node) {
            if (err) {
                rpc.fail(err);
                return;
            }
            if (node.data && node.data === READ_ONLY) {
                log.debug({
                    bucket: b,
                    key: k,
                    opts: opts,
                    node: node
                }, 'delObject: failed vnode is read only');
                rpc.fail(new ReadOnlyError());
                return;
            }

            var pnode = node.pnode;
            var client = options.clients.map[pnode];

            // if (isReadOnlyPnode(log, options.indexShards, pnode)) {
            //     rpc.fail(new ReadOnlyError(pnode));
            //     return;
            // }

            client.delObject(b, k, opts, function (dErr) {
                log.debug({
                    err: dErr
                }, 'delObject: returned');

                dtrace['delobject-done'].fire(function () {
                    return ([msgid]);
                });

                if (dErr) {
                    rpc.fail(dErr);
                } else {
                    rpc.end();
                }
            });
        });
    }

    return _delObject;
}


// function findObjects(options) {
//     assert.object(options, 'options');
//     assert.object(options.log, 'options.log');
//     assert.object(options.clients, 'options.clients');

//     function _findObjects(rpc) {
//         var msgid = rpc.requestId();
//         var argv = rpc.argv();

//         if (invalidArgs(rpc, argv, FO_ARGS_SCHEMA)) {
//             return;
//         }

//         var b = argv[0];
//         var f = argv[1];
//         var opts = argv[2];

//         var id = opts.req_id || uuid.v1();

//         dtrace['findobjects-start'].fire(function () {
//             return ([msgid, id, b, f]);
//         });

//         var log = options.log.child({
//             req_id: id
//         });

//         log.debug({
//             bucket: b,
//             filter: f,
//             opts: opts
//         }, 'find: entered');

//         if (!opts.hashkey && !opts.token) {
//             var errMsg = 'Invalid search request, requires either token or ' +
//                          'hashkey';
//             log.debug({
//                 bucket: b,
//                 filter: f,
//                 opts: opts
//             }, errMsg);

//             dtrace['findobjects-done'].fire(function () {
//                 return ([msgid, -1]);
//             });

//             rpc.fail(new Error(errMsg));
//             return;
//         }

//         var client;
//         if (opts.token) {
//             client = opts.token ? options.clients.map[opts.token] : null;
//             if (!client) {
//                 log.debug({token: opts.token}, 'findObject: Invalid Token');
//                 dtrace['findobjects-done'].fire(function () {
//                     return ([msgid, -1]);
//                 });
//                 rpc.fail(new Error('Invalid Token ' + opts.token));
//                 return;
//             }
//             processRequest();
//         } else {
//             // just pass in the key, no transformation is needed since the
//             // hashkey is explicitly specified here.
//             options.ring.getNodeNoSchema(opts.hashkey, function (err, node) {
//                 log.debug({
//                     err: err,
//                     node: node
//                 }, 'find: returned from getNodeNoSchema');
//                 if (err) {
//                     dtrace['findobjects-done'].fire(function () {
//                         return ([msgid, -1]);
//                     });
//                     rpc.fail(err);
//                     return;
//                 }
//                 client = options.clients.map[node.pnode];
//                 processRequest();
//             });
//         }

//         function processRequest() {
//             var req = client.findObjects(b, f, opts);

//             req.once('error', function (err) {
//                 log.debug({
//                     err: err
//                 }, 'findObject: done');
//                 dtrace['findobjects-done'].fire(function () {
//                     return ([msgid, -1]);
//                 });
//                 rpc.fail(err);
//             });

//             var total = 0;
//             req.on('record', function (obj) {
//                 total++;
//                 log.debug({
//                     obj: obj
//                 }, 'findObject: gotRecord');
//                 // delete the vnode from the value, as the vnode is only used
//                 // internally
//                 if (obj && obj.value) {
//                     delete obj.value.vnode;
//                 }
//                 if (obj && obj._value) {
//                     delete obj._value.vnode;
//                 }
//                 dtrace['findobjects-record'].fire(function () {
//                     return ([msgid,
//                         obj.key,
//                         obj._id,
//                         obj._etag,
//                     obj._value]);
//                 });
//                 rpc.write(obj);
//             });

//             req.on('end', function () {
//                 log.debug('findObject: done');
//                 dtrace['findobjects-done'].fire(function () {
//                     return ([msgid, total]);
//                 });
//                 rpc.end();
//             });
//         }
//     }

//     return _findObjects;
// }

/*
 * We don't currently support deleteMany operations.
 */
function deleteMany(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _deleteMany(rpc) {
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, DM_ARGS_SCHEMA)) {
            return;
        }

        var b = argv[0];
        var f = argv[1];
        var opts = argv[2];

        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            filter: f,
            opts: opts
        }, 'deleteMany: entered');
        rpc.fail(new Error('Operation not supported'));
    }

    return _deleteMany;
}

// function getTokens(options) {
//     assert.object(options, 'options');
//     assert.object(options.log, 'options.log');
//     assert.object(options.clients, 'options.clients');

//     function _getTokens(rpc) {
//         var argv = rpc.argv();

//         if (invalidArgs(rpc, argv, GT_ARGS_SCHEMA)) {
//             return;
//         }

//         var opts = argv[0];

//         var id = opts.req_id || uuid.v1();
//         var log = options.log.child({
//             req_id: id
//         });

//         log.debug({
//             opts: opts
//         }, 'getTokens: entered');

//         options.ring.getPnodes(function (err, pnodes) {
//             if (err) {
//                 rpc.fail(new verror.VError(err, 'unable to get pnodes'));
//                 return;
//             }
//             log.debug({
//                 pnodes: pnodes
//             }, 'getTokens: returned');
//             rpc.write(pnodes);
//             rpc.end();
//         });
//     }

//     return _getTokens;
// }

function sql(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _sql(rpc) {
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, SQL_ARGS_SCHEMA)) {
            return;
        }

        var stmt = argv[0];
        var values = argv[1];
        var opts = argv[2];

        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            stmt: stmt,
            values: values,
            opts: opts
        }, 'sql: entered');

        // if (options.ring.ro_ && !opts.readOnlyOverride) {
        //     log.debug({
        //         opts: opts,
        //         ro: options.ring.ro_
        //     }, 'sql: failed shard is read only');
        //     rpc.fail(new ReadOnlyError());
        //     return;
        // }

        var err = [];

        var barrier = vasync.barrier();
        barrier.on('drain', function () {
            var multiError = err[0] ? new verror.MultiError(err) : null;
            log.debug({
                err: multiError,
                stmt: stmt,
                values: values,
                opts: opts
            }, 'sql: finished all shards');
            if (multiError) {
                rpc.fail(multiError);
            } else {
                rpc.end();
            }
        });

        var readOnlyPnodes = listReadOnlyPnodes(log, options);
        if (readOnlyPnodes.length > 0) {
            rpc.fail(new ReadOnlyError(readOnlyPnodes.join(', ')));
            return;
        }

        options.clients.array.forEach(function (client, index) {
            barrier.start(index);
            var req = client.sql(stmt, values, opts);

            req.on('record', function (rec) {
                rpc.write(rec);
            });

            req.on('error', function (err2) {
                err.push(err2);
                barrier.done(index);
            });

            req.on('end', function () {
                barrier.done(index);
            });
        });
    }

    return _sql;
}

/*
 * We don't currently support update operations.
 */
function updateObjects(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _updateObjects(rpc) {
        var argv = rpc.argv();

        if (invalidArgs(rpc, argv, UO_ARGS_SCHEMA)) {
            return;
        }

        var b = argv[0];
        var fields = argv[1];
        var f = argv[2];
        var opts = argv[3];

        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            fields: fields,
            filter: f,
            opts: opts
        }, 'update: entered');

        rpc.fail(new Error('Operation not supported'));
    }

    return _updateObjects;
}


/*
 * Batching is only supported in a very limited case: for modifications when all
 * of the requests have keys with the same value after transformation (if
 * appropriate for their bucket), which allows us to be sure that all of the
 * values live on the same shard.
 *
 * The operations 'update' and 'deleteMany' are not allowed since they cannot be
 * guaranteed to only affect values on a single shard.
 */
// function batch(options) {
//     assert.object(options, 'options');
//     assert.object(options.log, 'options.log');
//     assert.object(options.clients, 'options.clients');

//     function _batch(rpc) {
//         var msgid = rpc.requestId();
//         var argv = rpc.argv();

//         if (invalidArgs(rpc, argv, B_ARGS_SCHEMA)) {
//             return;
//         }

//         var requests = argv[0];
//         var opts = argv[1];

//         var id = opts.req_id || uuid.v1();
//         var log = options.log.child({
//             req_id: id
//         });

//         log.debug({
//             requests: requests,
//             opts: opts
//         }, 'batch: entered');

//         dtrace['batch-start'].fire(function () {
//             return ([msgid, id]);
//         });

//         function done(err, meta) {
//             dtrace['batch-done'].fire(function () {
//                 return ([msgid]);
//             });

//             if (err) {
//                 log.debug(err, 'batch: failed');
//                 rpc.fail(err);
//             } else {
//                 log.debug({ meta: meta }, 'batch: done');
//                 rpc.write(meta);
//                 rpc.end();
//             }
//         }

//         if (!Array.isArray(requests) || requests.length === 0) {
//             done(new InvocationError('%s expects "requests" (args[0]) to be ' +
//                         'an array with at least one request but received an ' +
//                         'empty array', rpc.methodName()));
//             return;
//         }

//         for (var i = 0; i < requests.length; i++) {
//             var request = requests[i];

//             if (request.operation !== undefined && request.operation !== null &&
//                 ALLOWED_BATCH_OPS.indexOf(request.operation) === -1) {
//                 done(new Error(JSON.stringify(request.operation) +
//                     ' is not an allowed batch operation'));
//                 return;
//             }

//             if (typeof (request.key) !== 'string') {
//                 done(new Error('all batch requests must have a "key"'));
//                 return;
//             }

//             if (typeof (request.bucket) !== 'string') {
//                 done(new Error('all batch requests must have a "bucket"'));
//                 return;
//             }
//         }

//         options.ring.getNodeBatch(requests, function (err, node) {
//             if (err) {
//                 done(err);
//                 return;
//             }

//             if (node.data && node.data === READ_ONLY) {
//                 log.debug({
//                     requests: requests,
//                     opts: opts,
//                     node: node
//                 }, 'batch: failed vnode is read only');

//                 dtrace['batch-done'].fire(function () {
//                     return ([msgid]);
//                 });

//                 rpc.fail(new ReadOnlyError());
//                 return;
//             }

//             var pnode = node.pnode;
//             var client = options.clients.map[pnode];

//             // if (isReadOnlyPnode(log, options.indexShards, pnode)) {
//             //     rpc.fail(new ReadOnlyError(pnode));
//             //     return;
//             // }

//             client.batch(requests, opts, done);
//         });
//     }

//     return _batch;
// }

module.exports = {
    createServer: createServer
};
