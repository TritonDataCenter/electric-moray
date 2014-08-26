/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var clone = require('clone');
var uuid = require('node-uuid');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var FULL_CFG = {
    index: {
        str: {
            type: 'string'
        },
        str_u: {
            type: 'string',
            unique: true
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
    pre: [function onePre(req, cb) { cb(); }],
    post: [function onePost(req, cb) { cb(); }],
    options: {}
};



///--- Helpers

function assertBucket(name, t, bucket, cfg) {
    t.ok(bucket);
    if (!bucket)
        return (undefined);
    t.equal(bucket.name, name);
    t.ok(bucket.mtime instanceof Date);
    t.deepEqual(bucket.index, (cfg.index || {}));
    t.ok(Array.isArray(bucket.pre));
    t.ok(Array.isArray(bucket.post));
    t.equal(bucket.pre.length, (cfg.pre || []).length);
    t.equal(bucket.post.length, (cfg.post || []).length);

    if (bucket.pre.length !== (cfg.pre || []).length ||
    bucket.post.length !== (cfg.post || []).length)
    return (undefined);
    var i;
    for (i = 0; i < bucket.pre.length; i++)
        t.equal(bucket.pre[i].toString(), cfg.pre[i].toString());
    for (i = 0; i < bucket.post.length; i++)
        t.equal(bucket.post[i].toString(), cfg.post[i].toString());

    return (undefined);
}



///--- tests

before(function (cb) {
    this.bucket = 'moray_unit_test_' + uuid.v4().substr(0, 7);
    this.assertBucket = assertBucket.bind(null, this.bucket);

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


test('create bucket stock config', function (t) {
    var b = this.bucket;
    var c = this.client;

    c.createBucket(b, {}, function (err) {
        t.ok(err);
        t.equal(err.name, 'ReadOnlyError');
        t.end();
    });
});


test('update bucket', function (t) {
    var b = this.bucket;
    var c = this.client;
    c.updateBucket(b, {}, function (err) {
        t.ok(err);
        t.equal(err.name, 'ReadOnlyError');
        t.end();
    });
});


test('delete bucket', function (t) {
    var b = this.bucket;
    var c = this.client;

    c.deleteBucket(b, function (err) {
        t.ok(err);
        t.equal(err.name, 'ReadOnlyError');
        t.end();
    });
});


test('get bucket 404', function (t) {
    var c = this.client;
    c.getBucket(uuid.v4().substr(0, 7), function (err) {
        t.ok(err);
        t.equal(err.name, 'BucketNotFoundError');
        t.ok(err.message);
        t.end();
    });
});
