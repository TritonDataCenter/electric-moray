/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var clone = require('clone');
var uuidv4 = require('uuid/v4');
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');


/*
 * This test should be exercised by passing the electric-moray server a hash
 * ring which contains a single vnode ("0") using the -r flag, in addition to
 * testing a more standard ring.  Instructions for how to create a new hash ring
 * for testing can be found in the node-fash project's create documentation.
 */

///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var BUCKET_CFG = {
    index: {
        str: {
            type: 'string'
        },
        str_u: {
            type: 'string',
            unique: true
        },
        str_2: {
            type: 'string'
        },
        num: {
            type: 'number'
        },
        num_u: {
            type: 'number',
            unique: true
        },
        bool: {
            type: 'boolean'
        },
        bool_u: {
            type: 'boolean',
            unique: true
        }
    },
    pre: [function (req, cb) {
        var v = req.value;
        if (v.pre)
            v.pre = 'pre_overwrite';

        cb();
    }],
    post: [function (req, cb) {
        cb();
    }],
    options: {
        guaranteeOrder: true
    }
};



///--- Helpers

function assertObject(b, t, obj, k, v) {
    t.ok(obj);
    if (!obj)
        return (undefined);

    t.equal(obj.bucket, b);
    t.equal(obj.key, k);
    t.deepEqual(obj.value, v);
    t.ok(obj._id);
    t.ok(obj._etag);
    t.ok(obj._mtime);
    return (undefined);
}

///--- Tests

before(function (cb) {
    var self = this;
    this.bucket = 'testmanta';
    this.assertObject = assertObject.bind(this, this.bucket);
    this.client = helper.createClient();
    this.client.on('connect', function () {
        var b = self.bucket;
        self.client.createBucket(b, BUCKET_CFG, function (err) {
            if (err) {
                console.error(err.stack);
            }
            cb(err);
        });
    });
});


after(function (cb) {
    var self = this;
    this.client.delBucket(this.bucket, function (err) {
        if (err) {
            console.error(err.stack);
        }
        self.client.close();
        cb(err);
    });
});


test('get object 404', function (t) {
    var c = this.client;
    c.getObject(this.bucket, uuidv4().substr(0, 7), function (err) {
        t.ok(err);
        t.equal(err.name, 'ObjectNotFoundError');
        t.ok(err.message);
        t.end();
    });
});


test('del object 404', function (t) {
    var c = this.client;
    c.delObject(this.bucket, uuidv4().substr(0, 7), function (err) {
        t.ok(err);
        t.equal(err.name, 'ObjectNotFoundError');
        t.ok(err.message);
        t.end();
    });
});


test('CRUD object', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };
    var v2 = {
        str: 'hello world',
        pre: 'hi'
    };
    var self = this;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                self.assertObject(t, obj, k, v);
                return (cb());
            });
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                v2.pre = 'pre_overwrite';
                self.assertObject(t, obj, k, v2);
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('get object (cached)', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };
    var self = this;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, function (err, meta) {
                if (err)
                    return (cb(err));

                t.ok(meta);
                if (meta)
                    t.ok(meta.etag);
                return (cb());
            });
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                self.assertObject(t, obj, k, v);
                return (cb());
            });
        }, function getAgain(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                self.assertObject(t, obj, k, v);
                return (cb());
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


// can't realistically test this here at the electric-client. However, this is
// tested by the moray tests. Basically we enforce unique indices on a
// per-shard basis.
//test('CRUD objects unique indexes', function (t) {
    //var b = this.bucket;
    //var c = this.client;
    //var k = uuidv4();
    //var k2 = uuidv4();
    //var v = {
        //str_u: 'hi'
    //};
    //var v2 = {
        //str_u: 'hi'
    //};

    //vasync.pipeline({
        //funcs: [ function put(_, cb) {
            //c.putObject(b, k, v, cb);
        //}, function putFail(_, cb) {
            //c.putObject(b, k2, v2, function (err) {
                //t.ok(err);
                //t.equal(err.name, 'UniqueAttributeError');
                //cb();
            //});
        //}, function delK1(_, cb) {
            //c.delObject(b, k, cb);
        //}, function putK2(_, cb) {
            //c.putObject(b, k2, v2, cb);
        //}, function delK2(_, cb) {
            //c.delObject(b, k2, cb);
        //} ],
        //arg: {}
    //}, function (err) {
        //t.ifError(err);
        //t.end();
    //});
//});


test('put object w/etag ok', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };
    var v2 = {
        str: 'hello world'
    };
    var etag;
    var self = this;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                self.assertObject(t, obj, k, v);
                etag = obj._etag;
                return (cb());
            });
        }, function overwrite(_, cb) {
            c.putObject(b, k, v2, {etag: etag}, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                self.assertObject(t, obj, k, v2);
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('del object w/etag ok', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };
    var etag;
    var self = this;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function get(_, cb) {
            c.getObject(b, k, function (err, obj) {
                if (err)
                    return (cb(err));

                t.ok(obj);
                self.assertObject(t, obj, k, v);
                etag = obj._etag;
                return (cb());
            });
        }, function del(_, cb) {
            c.delObject(b, k, {etag: etag}, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('put object w/etag conflict', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function overwrite(_, cb) {
            c.putObject(b, k, {}, {etag: 'foo'}, function (err) {
                t.ok(err);
                if (err)
                    t.equal(err.name, 'EtagConflictError');
                cb();
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('del object w/etag conflict', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function drop(_, cb) {
            c.delObject(b, k, {etag: 'foo'}, function (err) {
                t.ok(err);
                if (err) {
                    t.equal(err.name, 'EtagConflictError');
                    t.ok(err.context);
                    if (err.context) {
                        var ctx = err.context;
                        t.equal(ctx.bucket, b);
                        t.equal(ctx.key, k);
                        t.equal(ctx.expected, 'foo');
                        t.ok(ctx.actual);
                    }
                }
                cb();
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('MANTA-980 - null etag support', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };
    var v2 = {
        str: 'hello world'
    };
    var etag;
    var value;
    var self = this;

    function get_cb(cb) {
        function _cb(err, obj) {
            if (err) {
                cb(err);
                return;
            }


            t.ok(obj);
            if (obj) {
                self.assertObject(t, obj, k, value);
                etag = obj._etag;
            }
            cb();
        }
        return (_cb);
    }

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            value = v;
            c.putObject(b, k, value, {etag: null}, cb);
        }, function get(_, cb) {
            c.getObject(b, k, get_cb(cb));
        }, function overwrite(_, cb) {
            value = v2;
            c.putObject(b, k, value, {etag: etag}, cb);
        }, function getAgain(_, cb) {
            c.getObject(b, k, {noCache: true}, get_cb(cb));
        }, function putFail(_, cb) {
            c.putObject(b, k, v, {etag: null}, function (err) {
                t.ok(err);
                if (err) {
                    t.equal(err.name, 'EtagConflictError');
                    t.ok(err.context);
                    t.equal(err.context.bucket, b);
                    t.equal(err.context.key, k);
                    t.equal(err.context.expected, 'null');
                    t.equal(err.context.actual, etag);
                }
                cb();
            });
        }, function del(_, cb) {
            c.delObject(b, k, {etag: etag}, cb);
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.end();
    });
});


test('find (like marlin)', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTokens(_, cb) {
            c.getTokens(function (err, res) {
                _.tokens = res.tokens;
                return cb(err);
            });
        }, function find(_, cb) {
            var f = '(&(str=hello)(!(str_2=usa)))';
            var count = 0;
            _.tokens.forEach(function (token) {
                var req = c.findObjects(b, f, {token: token});
                req.once('error', cb);
                req.once('end', function () {
                    if (++count === _.tokens.length) {
                        return cb();
                    }
                    return (undefined);
                });
                req.once('record', function (obj) {
                    t.ok(obj);
                    if (!obj)
                        return (undefined);

                    t.equal(obj.bucket, b);
                    t.equal(obj.key, k);
                    t.deepEqual(obj.value, v);
                    t.ok(obj._id);
                    t.ok(obj._count);
                    t.ok(obj._etag);
                    t.ok(obj._mtime);
                    found = true;
                    return (undefined);
                });
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find _mtime', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4();
    var now = Date.now();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTokens(_, cb) {
            c.getTokens(function (err, res) {
                _.tokens = res.tokens;
                return cb(err);
            });
        }, function find(_, cb) {
            var f = '(_mtime>=' + now + ')';
            var count = 0;
            _.tokens.forEach(function (token) {
                var req = c.findObjects(b, f, {token: token});
                req.once('error', cb);
                req.once('end', function () {
                    if (++count === _.tokens.length) {
                        return cb();
                    }
                    return (undefined);
                });
                req.once('record', function (obj) {
                    t.ok(obj);
                    if (!obj)
                        return (undefined);

                    t.equal(obj.bucket, b);
                    t.equal(obj.key, k);
                    t.deepEqual(obj.value, v);
                    t.ok(obj._id);
                    t.ok(obj._etag);
                    t.ok(obj._mtime);
                    found = true;
                    return (undefined);
                });
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find MANTA-156', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4();
    var v = {
        num: 0,
        num_u: 1
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTokens(_, cb) {
            c.getTokens(function (err, res) {
                _.tokens = res.tokens;
                return cb(err);
            });
        }, function find(_, cb) {
            var f = '(num>=0)';
            var count = 0;
            _.tokens.forEach(function (token) {
                var req = c.findObjects(b, f, {token: token});
                req.once('error', cb);
                req.once('end', function () {
                    if (++count === _.tokens.length) {
                        return cb();
                    }
                    return (undefined);
                });
                req.once('record', function (obj) {
                    t.ok(obj);
                    if (!obj)
                        return (undefined);

                    t.equal(obj.bucket, b);
                    t.equal(obj.key, k);
                    t.deepEqual(obj.value, v);
                    t.ok(obj._id);
                    t.ok(obj._etag);
                    t.ok(obj._mtime);
                    found = true;
                    return (undefined);
                });
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('find with hashkey', function (t) {
    var b = this.bucket;
    var c = this.client;
    var hashkey = uuidv4();
    var k = hashkey + '/' + uuidv4();
    var v = {
        num: 0,
        num_u: 1
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function find(_, cb) {
            var f = '(num>=0)';
            var req = c.findObjects(b, f, {hashkey: hashkey});
            req.once('error', cb);
            req.once('end', cb);
            req.once('record', function (obj) {
                t.ok(obj);
                if (!obj)
                    return (undefined);

                t.equal(obj.bucket, b);
                t.equal(obj.key, k);
                t.deepEqual(obj.value, v);
                t.ok(obj._id);
                t.ok(obj._etag);
                t.ok(obj._mtime);
                found = true;
                return (undefined);
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});

test('non-indexed AND searches (MANTA-317)', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4();
    var v = {
        str: 'hello',
        cow: 'moo'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTokens(_, cb) {
            c.getTokens(function (err, res) {
                _.tokens = res.tokens;
                return cb(err);
            });
        }, function find(_, cb) {
            var f = '(&(str=hello)(!(cow=woof)))';
            var count = 0;
            _.tokens.forEach(function (token) {
                var req = c.findObjects(b, f, {token: token});
                req.once('error', cb);
                req.once('end', function () {
                    if (++count === _.tokens.length) {
                        return cb();
                    }
                    return (undefined);
                });
                req.once('record', function (obj) {
                    t.ok(obj);
                    if (!obj)
                        return (undefined);

                    t.equal(obj.bucket, b);
                    t.equal(obj.key, k);
                    t.deepEqual(obj.value, v);
                    t.ok(obj._id);
                    t.ok(obj._etag);
                    t.ok(obj._mtime);
                    found = true;
                    return (undefined);
                });
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


test('_txn_snap on update', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4() + '/' + uuidv4();
    var v = {
        str: 'hi'
    };
    var txn;
    var self = this;

    vasync.pipeline({
        funcs: [ function create(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getOne(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    self.assertObject(t, obj, k, v);
                    t.ok(obj._txn_snap);
                    txn = obj._txn_snap;
                    cb();
                }
            });
        }, function update(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTwo(_, cb) {
            c.getObject(b, k, {noCache: true}, function (err, obj) {
                if (err) {
                    cb(err);
                } else {
                    t.ok(obj);
                    self.assertObject(t, obj, k, v);
                    t.ok(obj._txn_snap);
                    t.notEqual(txn, obj._txn_snap);
                    t.ok(obj._txn_snap > txn);
                    cb();
                }
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);

        t.end();
    });
});


test('find _txn_snap', function (t) {
    var b = this.bucket;
    var c = this.client;
    var k = uuidv4();
    var v = {
        str: 'hello',
        str_2: 'world'
    };
    var found = false;

    vasync.pipeline({
        funcs: [ function wait(_, cb) {
            setTimeout(cb, 500);
        }, function put(_, cb) {
            c.putObject(b, k, v, cb);
        }, function getTokens(_, cb) {
            c.getTokens(function (err, res) {
                _.tokens = res.tokens;
                return cb(err);
            });
        }, function find(_, cb) {
            var f = '(&(_txn_snap>=1)(_id>=1))';
            var count = 0;
            _.tokens.forEach(function (token) {
                var req = c.findObjects(b, f, {token: token});
                req.once('error', cb);
                req.once('end', function () {
                    if (++count === _.tokens.length) {
                        return cb();
                    }
                    return (undefined);
                });
                req.once('record', function (obj) {
                    t.ok(obj);
                    if (!obj)
                        return (undefined);

                    t.equal(obj.bucket, b);
                    t.equal(obj.key, k);
                    t.deepEqual(obj.value, v);
                    t.ok(obj._id);
                    t.ok(obj._etag);
                    t.ok(obj._mtime);
                    t.ok(obj._txn_snap);
                    found = true;
                    return (undefined);
                });
            });
        } ],
        arg: {}
    }, function (err) {
        t.ifError(err);
        t.ok(found);
        t.end();
    });
});


// we don't support batch operations
test('unsupported batch and update operations', function (t) {
    var b = this.bucket;
    var c = this.client;
    var self = this;
    var requests = [];
    for (var i = 0; i < 10; i++) {
        requests.push({
            bucket: self.bucket,
            key: uuidv4().substr(0, 7),
            value: {
                num: 20,
                num_u: i,
                str: 'foo',
                str_u: uuidv4().substr(0, 7)
            }
        });
    }

    c.batch(requests, function (put_err) {
        t.ok(put_err);

        c.updateObjects(b, {}, '(num>=20)', function (err) {
            t.ok(err);
            t.end();
        });
    });
});

test('unsupported deletemany', function (t) {
        var b = this.bucket;
        var c = this.client;
        c.deleteMany(b, '(num>=20)', function (err) {
            t.ok(err);
            t.end();
        });
});
