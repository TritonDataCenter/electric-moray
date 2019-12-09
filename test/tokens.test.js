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
    return (undefined);
}

///--- Tests

before(function (cb) {
    //this.bucket = 'moray_unit_test_' + uuidv4().substr(0, 7);
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

    c.getTokens(function (err, tokens) {
        t.ok(tokens);
        t.end();
    });
});
