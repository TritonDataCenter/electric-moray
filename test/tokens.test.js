// Copyright 2012 Joyent.  All rights reserved.

var clone = require('clone');
var uuid = require('node-uuid');
var vasync = require('vasync');

if (require.cache[__dirname + '/helper.js'])
    delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;



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
    if (v.vnode) {
        t.ok(obj.value.vnode);
    }
    return (undefined);
}

///--- Tests

before(function (cb) {
    var self = this;
    //this.bucket = 'moray_unit_test_' + uuid.v4().substr(0, 7);
    this.bucket = 'manta';
    this.assertObject = assertObject.bind(this, this.bucket);
    this.client = helper.createClient();
    this.client.on('connect', function () {
        cb();
    });
});


after(function (cb) {
    var self = this;
    self.client.close();
    return cb();
});


test('get tokens', function (t) {
    var c = this.client;
    var self = this;

    c.getTokens(function(err, tokens) {
        t.ok(tokens);
        t.end();
    });
});
