/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

var uuidv4 = require('uuid/v4');

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


///--- Tests

before(function (cb) {
        this.bucket = 'moray_unit_test_' + uuidv4().substr(0, 7);

        this.client = helper.createClient();
        this.client.on('connect', cb);

});

after(function (cb) {
        var self = this;
        // May or may not exist, just blindly ignore
        this.client.delBucket(this.bucket, function () {
                self.client.close();
                cb();
        });
});


test('MANTA-117 single quotes not being escaped', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuidv4();
        var cfg = {
                index: {
                        name: {
                                type: 'string',
                                unique: true
                        }
                }
        };
        var data = {
                name: uuidv4(),
                chain: [ {
                        name: 'A Task',
                        timeout: 30,
                        retry: 3,
                        body: function (job, cb) {
                                return cb(null);
                        }.toString()
                }],
                timeout: 180,
                onerror: [ {
                        name: 'Fallback task',
                        body: function (job, cb) {
                                return cb('Workflow error');
                        }.toString()
                }]
        };

        Object.keys(data).forEach(function (p) {
                if (typeof (data[p]) === 'object')
                        data[p] = JSON.stringify(data[p]);
        });

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        c.putObject(b, k, data, function (err3) {
                                t.ifError(err3);
                                t.end();
                        });
                });
        });
});

test('MANTA-328 numeric values in filters', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuidv4();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 123
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var ok = false;
                        var f = '(num=123)';
                        var req = c.findObjects(b, f, {hashkey: k});
                        req.once('error', function (err) {
                                t.ifError(err);
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                        req.once('record', function (obj) {
                                t.ok(obj);
                                t.equal(obj.bucket, b);
                                t.equal(obj.key, k);
                                t.deepEqual(obj.value, data);
                                t.ok(obj._id);
                                t.ok(obj._etag);
                                t.ok(obj._mtime);
                                ok = true;
                        });
                });
        });
});

test('MANTA-328 numeric values in filters <=', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuidv4();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 425
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var ok = false;
                        var f = '(num<=1024)';
                        var req = c.findObjects(b, f, {hashkey: k});
                        req.once('error', function (err) {
                                t.ifError(err);
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                        req.once('record', function (obj) {
                                t.ok(obj);
                                t.equal(obj.bucket, b);
                                t.equal(obj.key, k);
                                t.deepEqual(obj.value, data);
                                t.ok(obj._id);
                                t.ok(obj._etag);
                                t.ok(obj._mtime);
                                ok = true;
                        });
                });
        });
});


test('MANTA-328 numeric values in filters >=', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuidv4();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 425
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var ok = false;
                        var f = '(num>=81)';
                        var req = c.findObjects(b, f, {hashkey: k});
                        req.once('error', function (err) {
                                t.ifError(err);
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                        req.once('record', function (obj) {
                                t.ok(obj);
                                t.equal(obj.bucket, b);
                                t.equal(obj.key, k);
                                t.deepEqual(obj.value, data);
                                t.ok(obj._id);
                                t.ok(obj._etag);
                                t.ok(obj._mtime);
                                ok = true;
                        });
                });
        });
});


test('MANTA-170 bogus filter', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuidv4();
        var cfg = {
                index: {
                        num: {
                                type: 'number'
                        }
                }
        };
        var data = {
                num: 425
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var f = '(num>81)';
                        var req = c.findObjects(b, f, {hashkey: k});
                        req.once('error', function (err) {
                                t.end();
                        });
                        req.once('end', function () {
                                t.ok(false);
                                t.end();
                        });
                });
        });
});


test('MANTA-680 boolean searches', function (t) {
        var b = this.bucket;
        var c = this.client;
        var k = uuidv4();
        var cfg = {
                index: {
                        b: {
                                type: 'boolean'
                        }
                }
        };
        var data = {
                b: true
        };

        c.putBucket(b, cfg, function (err1) {
                t.ifError(err1);
                c.putObject(b, k, data, function (err2) {
                        t.ifError(err2);
                        var f = '(b=true)';
                        var req = c.findObjects(b, f, {hashkey: k});
                        var ok = false;
                        req.once('record', function () {
                                ok = true;
                        });
                        req.once('end', function () {
                                t.ok(ok);
                                t.end();
                        });
                });
        });
});
