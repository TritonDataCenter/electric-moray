// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fast = require('fast');
var moray_client = require('moray'); // client
var uuid = require('node-uuid');



///--- API

function createServer(options) {
    assert.object(options, 'options');

    var log = options.log;
    var opts = {
        log: options.log
    };

    options.client.log = options.log.child({component: "moray_client"});
    opts.client = createClient(options.client);

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

function createBucket(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    var log = options.log;

    function _createBucket(name, cfg, opts, res) {
        log.debug({
            bucket: name,
            cfg: cfg,
            opts: opts
        }, 'createBucket: entered');
        options.client.createBucket(name, cfg, opts, function(err) {
            log.debug({
                err: err
            }, 'createBucket: returned');
            res.end(err);
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
        options.client.getBucket(bucket, function(err, bucket) {
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
        options.client.updateBucket(name, cfg, function(err) {
            log.debug({
                err: err
            }, 'updateBucket: returned');
            res.end(err);
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
        options.client.delBucket(name, function(err) {
            log.debug({
                err: err
            }, 'delBucket: returned');
            res.end(err);
        });
    };

    return _delBucket;
};

function putObject(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

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

        options.client.putObject(b, k, v, opts, function(err, meta) {
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
        var log = options.log.child({
            req_id: id
        });
        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'getObject: entered');

        options.client.getObject(b, k, opts, function(err, obj) {
            log.debug({
                err: err,
                obj: obj
            }, 'getObject: returned');
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
        var log = options.log.child({
            req_id: id
        });
        log.debug({
            bucket: b,
            key: k,
            opts: opts
        }, 'delObject: entered');

        options.client.delObject(b, k, opts, function(err) {
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

        options.client.updateObjects(b, fields, f, res, function(err, meta) {
            log.debug({
                err: err,
                meta: meta
            }, 'update: returned');

            res.end(err ? err : meta);
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

var createClient = function createClient(options) {
    var client = moray_client.createClient(options);
    return (client);
};
