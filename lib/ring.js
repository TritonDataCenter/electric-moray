// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fash = require('fash');
var verror = require('verror');

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
    deserializeRing: deserializeRing
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

        return callback(err, hashedNode);
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
