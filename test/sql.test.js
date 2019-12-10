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



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;

var table = makeid();



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


test('test sql create table', function (t) {
        var c = this.client;
        var query = 'CREATE TABLE ' + table + '(foo integer);';
        var r = c.sql(query);

        r.on('record', function (rec) { });

        r.once('end', function () {
                t.done();
        });

        r.once('error', function (err) {
                t.fail(err);
                t.done();
        });
});

test('test sql update', function (t) {
        var c = this.client;
        var query = 'insert into ' + table + ' values (100);';
        var r = c.sql(query);

        r.on('record', function (rec) { });

        r.once('end', function () {
                t.done();
        });

        r.once('error', function (err) {
                t.fail(err);
                t.done();
        });
});


test('test sql get', function (t) {
        var c = this.client;
        var query = 'select * from ' + table + ' where foo=100;';
        var r = c.sql(query);

        r.on('record', function (rec) {
                t.ok(rec);
                t.ok(rec.foo);
                t.equal(rec.foo, 100);
        });

        r.once('end', function () {
                t.done();
        });

        r.once('error', function (err) {
                t.fail(err);
                t.done();
        });
});

///--- Helpers
function makeid() {
        var text = '';
        /* JSSTYLED */
        var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

        for (var i = 0; i < 10; i++)
        text += possible.charAt(Math.floor(Math.random() * possible.length));

        return text;
}
