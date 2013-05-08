// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');
var moray_client = require('moray'); // client
var uuid = require('node-uuid');
var url = require('url');
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

    opts.ring = ring.deserializeRing({
        log: options.log,
        topology: options.ring
    });

    log.info('creating moray clients');
    opts.clients = createClient({
        ring: opts.ring,
        log: options.log
    });

    var server = fast.createServer(opts);

    server.rpc('createBucket', createBucket(opts));
    server.rpc('getBucket', getBucket(opts));
    server.rpc('updateBucket', updateBucket(opts));
    server.rpc('delBucket', delBucket(opts));
    server.rpc('deleteMany', deleteMany(opts));
    server.rpc('putObject', putObject(opts));
    server.rpc('batch', batch(opts));
    server.rpc('getObject', getObject(opts));
    server.rpc('delObject', delObject(opts));
    server.rpc('findObjects', findObjects(opts));
    server.rpc('updateObjects', updateObjects(opts));
    server.rpc('getTokens', getTokens(opts));
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

        var err = [];
        var done = 0;
        options.clients.array.forEach(function(client) {
            client.createBucket(name, cfg, opts, function(err2) {
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
        var pnode = options.ring.getNode(uuid(), uuid()).pnode;
        options.clients.map[pnode].getBucket(bucket, function(err, rbucket) {
            log.debug({
                err: err,
                bucket: rbucket
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
                rbucket.index = JSON.stringify(rbucket.index);
                rbucket.pre.forEach(function(fn, index) {
                    rbucket.pre[index] = fn.toString();
                });
                rbucket.pre = JSON.stringify(rbucket.pre);
                rbucket.post.forEach(function(fn, index) {
                    rbucket.post[index] = fn.toString();
                });
                rbucket.post = JSON.stringify(rbucket.post);
                rbucket.options = JSON.stringify(rbucket.options);
                rbucket.mtime = JSON.stringify(rbucket.mtime.toString());

                res.end(rbucket);
            }
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

        var err = [];
        var done = 0;
        options.clients.array.forEach(function(client) {
            client.updateBucket(name, cfg, function(err2) {
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

        var err = [];
        var done = 0;
        options.clients.array.forEach(function(client) {
            client.delBucket(name, function(err2) {
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
    }

    return _delBucket;
}

function putObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.clients, 'options.clients');

    function _putObject(b, k, v, opts, res) {
        var id = opts.req_id || uuid.v1();
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            key: k,
            value: v,
            opts: opts
        }, 'putObject: entered');

        var node = options.ring.getNode(b, k);
        v.vnode = node.vnode;
        var pnode = node.pnode;
        options.clients.map[pnode].putObject(b, k, v, opts, function(err, meta) {
            log.debug({
                err: err,
                meta: meta
            }, 'putObject: returned');

            res.end(err ? err : meta);
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
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'getObject: entered');

        var node = options.ring.getNode(b, k);
        var pnode = node.pnode;
        options.clients.map[pnode].getObject(b, k, opts, function(err, obj) {
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

            log.info({
                obj: obj
            }, 'sanitized object');
            res.end(err ? err : obj);
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
        var log = options.log.child({
            req_id: id
        });

        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'delObject: entered');

        var node = options.ring.getNode(b, k);
        options.clients.map[node.pnode].delObject(b, k, opts, function(err) {
            log.debug({
                err: err
            }, 'delObject: returned');

            res.end(err);
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

            res.end(new Error(errMsg));
            return (undefined);
        }

        var client;
        if (opts.token) {
            client = opts.token ? options.clients.map[opts.token] : null;
            if (!client) {
                log.debug({token: opts.token}, 'findObject: Invalid Token');
                res.end(new Error('Invalid Token ' + opts.token));
                return (undefined);
            }
        } else {
            // just pass in the key, no transformation is needed since the
            // hashkey is explicitly specified here.
            var node = options.ring.getNodeNoSchema(opts.hashkey);
            client = options.clients.map[node.pnode];
        }

        var req = client.findObjects(b, f, opts);

        req.once('error', function(err) {
            log.debug({
                err: err
            }, 'findObject: done');
            res.end(err);
        });

        req.on('record', function(obj) {
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
            res.write(obj);
        });

        req.on('end', function() {
            log.debug('findObject: done');
            res.end();
        });
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

        var pnodes = options.ring.getPnodes();

        log.debug({
            pnodes: pnodes
        }, 'getTokens: returned');
        res.end(pnodes);
    }

    return _getTokens;
}

function createClient(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.ring, 'options.ring');

    var log = options.log;

    var clientMap = {};
    var clientArray = [];

    var pnodes = options.ring.getPnodes();

    pnodes.forEach(function(pnode) {
        var pnodeUrl = url.parse(pnode);
        assert.string(pnodeUrl.port, 'pnodeUrl.port');
        assert.string(pnodeUrl.hostname, 'pnodeUrl.hostname');

        log.info({
            url: pnodeUrl
        }, 'creating moray client');

        var client = moray_client.createClient({
            log: options.log.child({
                component: 'moray-client-' + pnodeUrl.hostname
            }),
            host: pnodeUrl.hostname,
            port: parseInt(pnodeUrl.port, 10)
        });

        clientMap[pnode] = client;
        clientArray.push(client);
    });

    if (clientArray.length <= 0) {
        throw new Error('No moray clients exist!');
    }

    return {
        map: clientMap,
        array: clientArray
    };
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
//function updateObjects(options) {
    //assert.object(options, 'options');
    //assert.object(options.log, 'options.log');

    //function _updateObjects(b, fields, f, opts, res) {
        //var id = opts.req_id || uuid.v1();
        //var log = options.log.child({
            //req_id: id
        //});

        //log.debug({
            //bucket: b,
            //fields: fields,
            //filter: f,
            //opts: opts
        //}, 'update: entered');

        //var client = opts.token ? options.clients.map[opts.token] : null;
        //if (!client) {
            //log.debug({token: opts.token}, 'updateObject: Invalid Token');
            //res.end(new Error('Invalid Token ' + opts.token));
        //} else {
            //client.updateObjects(b, fields, f, res, function(err, meta) {
                //log.debug({
                    //err: err,
                    //meta: meta
                //}, 'update: returned');

                //res.end(err ? err : meta);
            //});
        //}
    //}

    //return _updateObjects;
//};

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
//function batch(options) {
    //assert.object(options, 'options');
    //assert.object(options.log, 'options.log');

    //function _batch(requests, opts, res) {
        //var id = opts.req_id || uuid.v1();
        //var log = options.log.child({
            //req_id: id
        //});

        //log.debug({
            //requests: requests,
            //opts: opts
        //}, 'batch: entered');

        //var client = opts.token ? options.clients.map[opts.token] : null;
        //if (!client) {
            //log.debug({token: opts.token}, 'batch: Invalid Token');
            //res.end(new Error('Invalid Token ' + opts.token));
        //} else {
            //client.batch(requests, opts, function(err, meta) {
                //log.debug({
                    //err: err,
                    //meta: meta
                //}, 'batch: returned');
                //res.end(err ? err : meta);
            //});
        //}

    //};

    //return _batch;
//};

