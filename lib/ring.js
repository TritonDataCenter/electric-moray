/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var dtrace = require('./dtrace');
var fash = require('fash');
var schema = require('./schema/index');


var READ_ONLY = 'ro';


function Ring(options, cb) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.chash, 'options.chash');
    assert.func(cb, 'callback');

    var self = this;

    this.log_ = options.log;
    this.chash_ = options.chash;
    this.ro_ = false;

    // check whether this has has r/o nodes
    self.chash_.getDataVnodes(function (err, vnodes) {
        if (err) {
            return cb(err);
        }

        // just assume if vnodes.length isn't 0, there's ro nodes
        if (vnodes.length > 0) {
            self.ro_ = true;
        }

        self.log_.info({ro: this.ro_}, 'Ring.new: instantiated Ring');
        return cb(null, self);
    });
}



///--- Exports

module.exports = {
    createRing: createRing,
    deserializeRing: deserializeRing,
    loadRing: loadRing
};



///--- API

/**
 * Gets the hashed node given a key and a bucket.
 * @param {String} bucket The bucket this key belongs to, if a schema exists
 * for this bucket, the key is transformed.
 * @param {String} key The key.
 * @param {Function} callback The callback of the type f(err, hashedNode).
 *
 */
Ring.prototype.getNode = function getNode(bucket, key, callback) {
    var self = this;
    var log = self.log_;

    log.debug({
        bucket: bucket,
        key: key
    }, 'Ring.getNode: entered');

    var tkey = schema.transformKey(bucket, key);

    log.debug({
        key: key,
        tkey: tkey
    }, 'Ring.getNode: key transformed');

    self.chash_.getNode(tkey, function (err, hashedNode) {
        log.debug({
            err: err,
            bucket: bucket,
            key: key,
            tkey: tkey,
            hashedNode: hashedNode
        }, 'Ring.getNode: exiting');

        dtrace['selected-pnode'].fire(function () {
            return ([hashedNode]);
        });

        return callback(err, hashedNode);
    });
};

/**
 * Gets the hashed node given a series of batch operations.
 *
 * @param {Array} requests An array of {bucket,key} objects which must all
 * transform to the same key to determine which node a series of operations
 * will performed on.
 * @param {Function} callback The callback of the type f(err, hashedNode).
 */
Ring.prototype.getNodeBatch = function getNodeBatch(origRequests, callback) {
    var self = this;
    var log = self.log_;
    var requests = origRequests.slice();

    log.debug({
        requests: requests
    }, 'Ring.getNodeBatch: entered');

    var request = requests.shift();
    var tkey = schema.transformKey(request.bucket, request.key);

    for (var i = 0; i < requests.length; i++) {
        var currKey = schema.transformKey(requests[i].bucket, requests[i].key);
        if (tkey !== currKey) {
            setImmediate(callback,
                new Error('all requests must transform to the same key'));
            return;
        }
    }

    log.debug({
        requests: requests,
        tkey: tkey
    }, 'Ring.getNodeBatch: key transformed');

    self.chash_.getNode(tkey, function (err, hashedNode) {
        log.debug({
            err: err,
            requests: requests,
            tkey: tkey,
            hashedNode: hashedNode
        }, 'Ring.getNodeBatch: exiting');

        dtrace['selected-pnode'].fire(function () {
            return ([hashedNode]);
        });

        callback(err, hashedNode);
    });
};

/**
 * Gets the hashed node given only a key. The key is not transformed in anyway.
 * @param {String} key The key.
 * @param {Function} callback The callback of the type f(err, hashedNode).
 *
 */
Ring.prototype.getNodeNoSchema = function getNodeNoSchema(key, callback) {
    var self = this;
    var log = self.log_;

    log.debug({
        key: key
    }, 'Ring.getNode: entered');

    self.chash_.getNode(key, function (err, hashedNode) {
        log.debug({
            err: err,
            key: key,
            hashedNode: hashedNode
        }, 'Ring.getNode: exiting');

        dtrace['selected-pnode'].fire(function () {
            return ([hashedNode]);
        });

        return callback(err, hashedNode);
    });
};

Ring.prototype.getPnodes = function getPnodes(callback) {
    var self = this;
    var log = self.log_;

    log.debug('Ring.getPnodes: entered');

    self.chash_.getPnodes(function (err, pnodes) {
        log.debug({err: err, pnodes: pnodes}, 'Ring.getPnodes: exiting');
        return callback(err, pnodes);
    });
};



///--- Privates


function loadRing(options, callback) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.location, 'options.location');
    assert.object(options.leveldbCfg, 'options.leveldbCfg');
    assert.func(callback, 'callback');

    options.log = options.log.child({component: 'ring'});

    // Loading the db from disk, so of course we want to turn this error off
    var leveldbCfg = {
        errorIfExists: false
    };

    options.ring = fash.load({
        log: options.log,
        backend: fash.BACKEND.LEVEL_DB,
        location: options.location,
        leveldbCfg: options.leveldbCfg || leveldbCfg
    }, function (err, chash) {
        if (err) {
            return callback(err);
        }
        options.chash = chash;

        var r = new Ring(options, function (_err) {
            return callback(err, r);
        });
    });
}

function createRing(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.arrayOfString(options.pnodes, 'options.pnodes');
    assert.number(options.vnodes, 'options.vnodes');
    assert.optionalString(options.algorithm, 'options.algorithm');

    options.log = options.log.child({component: 'ring'});

    options.ring = fash.create({
        log: options.log,
        algorithm: options.algorithm || fash.ALGORITHMS.SHA256,
        pnodes: options.pnodes,
        vnodes: options.vnodes
    });

    return new Ring(options);
}

function deserializeRing(options, callback) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.topology, 'options.topology');
    assert.string(options.location, 'options.location');

    options.log = options.log.child({component: 'fash'});

    fash.deserialize({
        log: options.log,
        topology: options.topology,
        backend: fash.BACKEND.LEVEL_DB,
        location: options.location
    }, function (err, chash) {
        if (err) {
            return callback(err);
        }
        options.chash = chash;

        var r = new Ring(options, function (_err) {
            return callback(err, r);
        });
    });
}
