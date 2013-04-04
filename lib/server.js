// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');
var moray_client = require('moray'); // client
var uuid = require('node-uuid');
var verror = require('verror');

var ring = require('./ring');


///--- API

function createServer(options) {
    assert.object(options, 'options');
    assert.object(options.ring, 'options.ring');

    var log = options.log;
    var opts = {
        log: options.log
    };

    options.client.log = options.log;
    opts.client = createClient(options.client);

    options.ring.log = options.log;
    opts.ring = ring.createRing(options.ring);

    var server = fast.createServer(opts);

    // TODO: figure out which shard each request should go to.
    server.rpc('createBucket', createBucket(opts));
    server.rpc('getBucket', getBucket(opts));
    server.rpc('updateBucket', updateBucket(opts));
    server.rpc('delBucket', delBucket(opts));
    server.rpc('putObject', putObject(opts));
    server.rpc('batch', batch(opts));
    server.rpc('getObject', getObject(opts));
    server.rpc('delObject', delObject(opts));
    server.rpc('findObjects', findObjects(opts));
    server.rpc('updateObjects', updateObjects(opts));
    //server.rpc('sql', sql.sql(opts));
    //server.rpc('ping', ping.ping(opts));

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

    server.on('error', function (err) {
        log.error(err, 'server error');
        process.exit(1);
    });

    server.listen(options.port, function () {
        log.info('moray listening on %d', options.port);
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

        var err = [];
        var done = 0;
        options.client.array.forEach(function(client) {
            client.createBucket(name, cfg, opts, function(err2) {
                log.debug({
                    err: err2,
                    client: client.host
                }, 'createBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.client.array.length) {
                    var multiError = err[0] ? new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'createBucket: finished all shards');

                    res.end(multiError);
                }
            });
        });

    };

    return _createBucket;
}

function getBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

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
        var pnode = options.ring.getNode(uuid(), uuid()).pnode;
        options.client.map[pnode].getBucket(bucket, function(err, bucket) {
            log.debug({
                err: err,
                bucket: bucket
            }, 'getBucket: returned');

            /* serialize the deserialized bucket response. To make this faster,
             * we could:
             * 1) modify the moray client to make deserializing optional.
             * 2) directly hook up the streams by modifying the underlying
             * node-fast stream.
             */
            if (err) {
                res.end(err);
            } else {
                bucket.index = JSON.stringify(bucket.index);
                bucket.pre.forEach(function(fn, index) {
                    bucket.pre[index] = fn.toString();
                });
                bucket.pre = JSON.stringify(bucket.pre);
                bucket.post.forEach(function(fn, index) {
                    bucket.post[index] = fn.toString();
                });
                bucket.post = JSON.stringify(bucket.post);
                bucket.options = JSON.stringify(bucket.options);
                bucket.mtime = JSON.stringify(bucket.mtime.toString());

                res.end(bucket);
            }
        });
    };

    return _getBucket;
};

function updateBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

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

        var err = [];
        var done = 0;
        options.client.array.forEach(function(client) {
            client.updateBucket(name, cfg, function(err2) {
                log.debug({
                    err: err2
                }, 'updateBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.client.array.length) {
                    var multiError = err[0] ? new verror.MultiError(err) : null;
                    console.log(multiError);
                    log.debug({
                        err: multiError
                    }, 'updateBucket: finished all shards');

                    res.end(multiError);
                }
            });
        });
    };

    return _updateBucket;
};

function delBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _delBucket(name, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: name,
            opts: opts
        }, 'delBucket: entered');

        var err = [];
        var done = 0;
        options.client.array.forEach(function(client) {
            client.delBucket(name, function(err2) {
                log.debug({
                    err: err2
                }, 'delBucket: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.client.array.length) {
                    var multiError = err[0] ? new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'delBucket: finished all shards');

                    res.end(multiError);
                }
            });
        });
    };

    return _delBucket;
};

function putObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _putObject(b, k, v, opts, res) {
        var id = opts.req_id || uuid.v1();
        var node = options.ring.getNode(b, k);
        var log = options.log.child({
            req_id: id,
            hashedNode: node
        });

        log.debug({
            bucket: b,
            key: k,
            value: v,
            opts: opts
        }, 'putObject: entered');

        v.vnode = node.vnode;
        var pnode = node.pnode;
        options.client.map[pnode].putObject(b, k, v, opts, function(err, meta) {
            log.debug({
                err: err,
                meta: meta
            }, 'putObject: returned');

            res.end(err ? err : meta);
        });
    };

    return _putObject;
};

function getObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _getObject(b, k, opts, res) {
        var id = opts.req_id || uuid.v1();
        var node = options.ring.getNode(b, k);
        var log = options.log.child({
            req_id: id,
            hashedNode: node
        });

        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'getObject: entered');

        var pnode = node.pnode;
        options.client.map[pnode].getObject(b, k, opts, function(err, obj) {
            log.debug({
                err: err,
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

            log.warn({
                obj: obj
            }, 'sanitized object');
            res.end(err ? err : obj);
        });
    };

    return _getObject;
};

function delObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _delObject(b, k, opts, res) {
        var id = opts.req_id || uuid.v1();
        var node = options.ring.getNode(b, k);
        var log = options.log.child({
            req_id: id,
            hashedNode: node
        });

        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'delObject: entered');

        options.client.map[node.pnode].delObject(b, k, opts, function(err) {
            log.debug({
                err: err
            }, 'delObject: returned');

            res.end(err);
        });
    };

    return _delObject;
};

var findObjects = function findObjects(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    var _findObjects = function _findObjects(b, f, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            filter: f,
            opts: opts
        }, 'find: entered');

        var req = options.client.findObjects(b, f, opts);

        req.once('error', function(err) {
            log.debug({
                err: err
            }, 'findObject: done');
            res.end(err);
        });

        req.on('record', function(obj) {
            log.debug({
                obj: obj
            }, 'findObject; gotRecord');
            res.write(obj);
        });

        req.on('end', function() {
            log.debug('findObject: done');
            res.end();
        });
    };

    return _findObjects;
};

function updateObjects(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

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

        var err = [];
        var done = 0;
        options.client.array.forEach(function(client) {
            client.updateObjects(b, fields, f, res, function(err2, meta) {
                log.debug({
                    err: err2,
                    meta: meta
                }, 'update: returned');

                if (err2) {
                    err.push(err2);
                }

                if (++done === options.client.array.length) {
                    var multiError = err[0] ? new verror.MultiError(err) : null;
                    log.debug({
                        err: multiError
                    }, 'updateOjbects: finished all shards');

                    res.end(multiError);
                }
            });
        });
    };

    return _updateObjects;
};

function batch(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    function _batch(requests, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            requests: requests,
            opts: opts
        }, 'batch: entered');

        options.client.batch(requests, opts, function(err, meta) {
            log.debug({
                err: err,
                meta: meta
            }, 'batch: returned');
            res.end(err ? err : meta);
        });
    };

    return _batch;
};

function createClient(options) {
    var clientMap = {};
    var clientArray = [];
    options.forEach(function(clientOpt) {
        clientOpt.log = options.log.child({
            component: 'moray-client-' + clientOpt.host
        });
        var client = moray_client.createClient(clientOpt);
        clientMap[clientOpt.host] = client;
        clientArray.push(client);
    });

    return {
        map: clientMap,
        array: clientArray
    };
};
