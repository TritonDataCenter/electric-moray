/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

var assert = require('assert-plus');
var fast = require('fast');
var fs = require('fs');
var moray_client = require('moray'); // client
var uuid = require('node-uuid');
var url = require('url');
var vasync = require('vasync');
var verror = require('verror');

var dtrace = require('./dtrace');
var ring = require('./ring');
require('./errors');



///--- GLOBALS

var READ_ONLY = 'ro';


///--- API

function createServer(options) {
    assert.object(options, 'options');
    assert.string(options.ringLocation, 'options.ringLocation');

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

    ring.loadRing({
        log: options.log,
        location: options.ringLocation,
        leveldbCfg: options.ringCfg.leveldbCfg
    }, function (err, _ring) {
        if (err) {
            throw new verror.VError(err, 'unable to instantiate hash ring');
        }

        opts.ring = _ring;

        log.info('creating moray clients');
        createClient({
            ring: opts.ring,
            log: options.log
        }, function (_err, clients) {
            if (_err) {
                throw new verror.VError(_err, 'unable to create moray clients');
            }

            opts.clients = clients;

            var server = fast.createServer(opts);

            server.rpc('batch', batch(opts));
            server.rpc('createBucket', createBucket(opts));
            server.rpc('delBucket', delBucket(opts));
            server.rpc('delObject', delObject(opts));
            server.rpc('deleteMany', deleteMany(opts));
            server.rpc('findObjects', findObjects(opts));
            server.rpc('getBucket', getBucket(opts));
            server.rpc('getObject', getObject(opts));
            server.rpc('getTokens', getTokens(opts));
            server.rpc('putObject', putObject(opts));
            server.rpc('sql', sql(opts));
            server.rpc('updateBucket', updateBucket(opts));
            server.rpc('updateObjects', updateObjects(opts));

            if (options.audit !== false) {
                server.on('after', function (name, req, res) {
                    var t = Math.floor(res.elapsed / 1000);
                    var obj = {
                        method: name,
                        'arguments': req,
                        serverTime: t + 'ms'
                    };

                    log.info(obj, 'request handled');
                });
            }

            server.on('error', function (__err) {
                log.error(__err, 'server error');
                process.exit(1);
            });

            server.listen(options.port, function () {
                log.info('moray listening on %d', options.port);
            });
        });
    });
}



///--- Exports

module.exports = {
    createServer: createServer
};



///--- Privates

//TODO; No bucket modification can occur whilst re-sharding.
function createBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _createBucket(name, cfg, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: name,
            cfg: cfg,
            opts: opts
        }, 'createBucket: entered');

        if (options.ring.ro_) {
            log.debug({
                bucket: name,
                cfg: cfg,
                opts: opts,
                ro: options.ring.ro_
            }, 'createBucket: failed shard is read only');
            return res.end(new ReadOnlyError());
        }
        var err = [];
        var done = 0;
        options.clients.array.forEach(function (client) {
            client.createBucket(name, cfg, opts, function (err2) {
                log.debug({
                    err: err2,
                    client: client.host
                }, 'createBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.clients.array.length) {
                    var multiError = err[0] ? new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'createBucket: finished all shards');

                    res.end(multiError);
                }
            });
        });

        return (undefined);
    }

    return _createBucket;
}

function getBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _getBucket(opts, bucket, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: bucket,
            opts: opts
        }, 'getBucket: entered');

        // randomly pick a client -- since all bucket configs are the same;
        options.ring.getNode(uuid(), uuid(), function (err, hNode) {
            if (err) {
                res.end(err);
                return (undefined);
            }

            var pnode = hNode.pnode;
            options.clients.map[pnode].getBucket(bucket,
                                                 function (_err, rbucket)
            {
                log.debug({
                    err: _err,
                    bucket: rbucket
                }, 'getBucket: returned');

                /*
                 * serialize the deserialized bucket response. To make this
                 * faster, we could:
                 * 1) modify the moray client to make deserializing optional.
                 * 2) directly hook up the streams by modifying the underlying
                 * node-fast stream.
                 */
                if (_err) {
                    res.end(_err);
                } else {
                    rbucket.index = JSON.stringify(rbucket.index);
                    rbucket.pre.forEach(function (fn, index) {
                        rbucket.pre[index] = fn.toString();
                    });
                    rbucket.pre = JSON.stringify(rbucket.pre);
                    rbucket.post.forEach(function (fn, index) {
                        rbucket.post[index] = fn.toString();
                    });
                    rbucket.post = JSON.stringify(rbucket.post);
                    rbucket.options = JSON.stringify(rbucket.options);
                    rbucket.mtime = JSON.stringify(rbucket.mtime.toString());
                    if (rbucket.hasOwnProperty('reindex_active')) {
                        rbucket.reindex_active = JSON.stringify(
                            rbucket.reindex_active);
                    }

                    res.end(rbucket);
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

    function _updateBucket(name, cfg, opts, res) {
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
            return res.end(new ReadOnlyError());
        }

        var err = [];
        var done = 0;
        options.clients.array.forEach(function (client) {
            client.updateBucket(name, cfg, function (err2) {
                log.debug({
                    err: err2
                }, 'updateBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.clients.array.length) {
                    var multiError = err[0] ? new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'updateBucket: finished all shards');

                    res.end(multiError);
                }
            });
        });

        return (undefined);
    }

    return _updateBucket;
}

function delBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _delBucket(name, opts, res) {
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
            return res.end(new ReadOnlyError());
        }

        var err = [];
        var done = 0;
        options.clients.array.forEach(function (client) {
            client.delBucket(name, function (err2) {
                log.debug({
                    err: err2
                }, 'delBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.clients.array.length) {
                    var multiError = err[0] ? new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'delBucket: finished all shards');

                    res.end(multiError);
                }
            });
        });

        return (undefined);
    }

    return _delBucket;
}

function putObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _putObject(b, k, v, opts, res) {
        var id = opts.req_id || uuid.v1();

        dtrace['putobject-start'].fire(function () {
            return ([res.msgid, id, b, k, opts._value]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            key: k,
            value: v,
            opts: opts
        }, 'putObject: entered');

        options.ring.getNode(b, k, function (err, node) {
            if (err) {
                res.end(err);
                return (undefined);
            }
            if (node.data && node.data === READ_ONLY) {
                log.debug({
                    bucket: b,
                    key: k,
                    value: v,
                    opts: opts,
                    node: node
                }, 'putObject: failed vnode is read only');
                return res.end(new ReadOnlyError());
            }
            v.vnode = node.vnode;
            var pnode = node.pnode;
            var client = options.clients.map[pnode];
            client.putObject(b, k, v, opts, function (_err, meta) {
                log.debug({
                    err: _err,
                    meta: meta
                }, 'putObject: returned');

                dtrace['putobject-done'].fire(function () {
                    return ([res.msgid]);
                });
                res.end(_err ? _err : meta);
            });

            return (undefined);
        });
    }

    return _putObject;
}


function getObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _getObject(b, k, opts, res) {
        var id = opts.req_id || uuid.v1();

        dtrace['getobject-start'].fire(function () {
            return ([res.msgid, id, b, k]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'getObject: entered');

        options.ring.getNode(b, k, function (err, node) {
            if (err) {
                res.end(err);
                return (undefined);
            }
            var pnode = node.pnode;
            var client = options.clients.map[pnode];
            client.getObject(b, k, opts, function (_err, obj) {
                log.debug({
                    err: _err,
                    obj: obj
                }, 'getObject: returned');

                // delete the vnode from the value, as the vnode is only used
                // internally
                if (obj && obj.value && obj.value.vnode) {
                    delete obj.value.vnode;
                }
                if (obj && obj._value && obj._value.vnode) {
                    delete obj._value.vnode;
                }

                // MANTA-1400: set the vnode info for debugging purposes
                if (obj) {
                    obj._node = node;
                }

                log.debug({
                    obj: obj
                }, 'sanitized object');

                dtrace['getobject-done'].fire(function () {
                    return ([res.msgid, obj]);
                });
                res.end(_err ? _err : obj);
            });
        });
    }

    return _getObject;
}

function delObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _delObject(b, k, opts, res) {
        var id = opts.req_id || uuid.v1();

        dtrace['delobject-start'].fire(function () {
            return ([res.msgid, id, b, k]);
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
                res.end(err);
                return (undefined);
            }
            if (node.data && node.data === READ_ONLY) {
                log.debug({
                    bucket: b,
                    key: k,
                    opts: opts,
                    node: node
                }, 'delObject: failed vnode is read only');
                return res.end(new ReadOnlyError());
            }
            var client = options.clients.map[node.pnode];
            client.delObject(b, k, opts, function (_err) {
                log.debug({
                    err: _err
                }, 'delObject: returned');

                dtrace['delobject-done'].fire(function () {
                    return ([res.msgid]);
                });
                res.end(_err);
            });

            return (undefined);
        });
    }

    return _delObject;
}


function findObjects(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _findObjects(b, f, opts, res) {
        var id = opts.req_id || uuid.v1();

        dtrace['findobjects-start'].fire(function () {
            return ([res.msgid, id, b, f]);
        });

        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            filter: f,
            opts: opts
        }, 'find: entered');

        if (!opts.hashkey && !opts.token) {
            var errMsg = 'Invalid search request, requires either token or ' +
                         'hashkey';
            log.debug({
                bucket: b,
                filter: f,
                opts: opts
            }, errMsg);

            dtrace['findobjects-done'].fire(function () {
                return ([res.msgid, -1]);
            });

            res.end(new Error(errMsg));
            return (undefined);
        }

        var client;
        if (opts.token) {
            client = opts.token ? options.clients.map[opts.token] : null;
            if (!client) {
                log.debug({token: opts.token}, 'findObject: Invalid Token');
                dtrace['findobjects-done'].fire(function () {
                    return ([res.msgid, -1]);
                });
                res.end(new Error('Invalid Token ' + opts.token));
                return (undefined);
            }
            processRequest();
        } else {
            // just pass in the key, no transformation is needed since the
            // hashkey is explicitly specified here.
            options.ring.getNodeNoSchema(opts.hashkey, function (err, node) {
                log.debug({
                    err: err,
                    node: node
                }, 'find: returned from getNodeNoSchema');
                if (err) {
                    dtrace['findobjects-done'].fire(function () {
                        return ([res.msgid, -1]);
                    });
                    res.end(err);
                    return (undefined);
                }
                client = options.clients.map[node.pnode];
                processRequest();
            });
        }

        function processRequest() {
            var req = client.findObjects(b, f, opts);

            req.once('error', function (err) {
                log.debug({
                    err: err
                }, 'findObject: done');
                dtrace['findobjects-done'].fire(function () {
                    return ([res.msgid, -1]);
                });
                res.end(err);
            });

            var total = 0;
            req.on('record', function (obj) {
                total++;
                log.debug({
                    obj: obj
                }, 'findObject: gotRecord');
                // delete the vnode from the value, as the vnode is only used
                // internally
                if (obj && obj.value && obj.value.vnode) {
                    delete obj.value.vnode;
                }
                if (obj && obj._value && obj._value.vnode) {
                    delete obj._value.vnode;
                }
                dtrace['findobjects-record'].fire(function () {
                    return ([res.msgid,
                        obj.key,
                        obj._id,
                        obj._etag,
                    obj._value]);
                });
                res.write(obj);
            });

            req.on('end', function () {
                log.debug('findObject: done');
                dtrace['findobjects-done'].fire(function () {
                    return ([res.msgid, total]);
                });
                res.end();
            });

            return (undefined);
        }
        return (undefined);
    }

    return _findObjects;
}

function deleteMany(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _deleteMany(b, f, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            filter: f,
            opts: opts
        }, 'deleteMany: entered');
        res.end(new Error('Operation not supported'));
    }

    return _deleteMany;
}

function getTokens(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _getTokens(opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            opts: opts,
            res: res
        }, 'getTokens: entered');

        options.ring.getPnodes(function (err, pnodes) {
            if (err) {
                return res.end(new verror.VError(err, 'unable to get pnodes'));
            }
            log.debug({
                pnodes: pnodes
            }, 'getTokens: returned');
            res.end(pnodes);
        });

    }

    return _getTokens;
}

function createClient(options, callback) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.ring, 'options.ring');
    assert.func(callback, 'options.callback');

    var log = options.log;

    var clientMap = {};
    var clientArray = [];

    options.ring.getPnodes(function (err, pnodes) {
        if (err) {
            throw new verror.VError(err, 'unable to get pnodes');
        }
        pnodes.forEach(function (pnode) {
            var pnodeUrl = url.parse(pnode);
            assert.string(pnodeUrl.port, 'pnodeUrl.port');
            assert.string(pnodeUrl.hostname, 'pnodeUrl.hostname');

            log.info({
                url: pnodeUrl
            }, 'creating moray client');

            var client = moray_client.createClient({
                unwrapErrors: true,
                log: options.log.child({
                    component: 'moray-client-' + pnodeUrl.hostname
                }),
                host: pnodeUrl.hostname,
                port: parseInt(pnodeUrl.port, 10)
            });

            clientMap[pnode] = client;
            clientArray.push(client);

            if (clientArray.length === pnodes.length) {
                // write ready cookie when clients have connected
                log.info('all moray clients instantiated writing ready cookie');
                try {
                    fs.writeFileSync('/var/tmp/electric-moray-ready', null);
                } catch (e) {
                    throw new verror.VError(e, 'unable to write ready cookie');
                }
            }
        });

        if (clientArray.length <= 0) {
            throw new verror.VError('No moray clients exist!');
        }

        return callback(null, {
            map: clientMap,
            array: clientArray
        });
    });
}

function sql(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _sql(stmt, values, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            stmt: stmt,
            values: values,
            opts: opts
        }, 'sql: entered');

        if (options.ring.ro_) {
            log.debug({
                opts: opts,
                ro: options.ring.ro_
            }, 'sql: failed shard is read only');
            return res.end(new ReadOnlyError());
        }

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
            res.end(multiError);
        });

        options.clients.array.forEach(function (client, index) {
            barrier.start(index);
            var req = client.sql(stmt, values, opts);

            req.on('record', function (rec) {
                res.write(rec);
            });

            req.on('error', function (err2) {
                err.push(err2);
                barrier.done(index);
            });

            req.on('end', function () {
                barrier.done(index);
            });
        });

        return (undefined);
    }

    return _sql;
}

// we don't currently support update operations
function updateObjects(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _updateObjects(b, fields, f, opts, res) {
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

        res.end(new Error('Operation not supported'));
    }

    return _updateObjects;
}

// we don't support batch operations
function batch(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _batch(requests, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            requests: requests,
            opts: opts
        }, 'batch: entered');

        res.end(new Error('Operation not supported'));
    }

    return _batch;
}
