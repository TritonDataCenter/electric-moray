// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fash = require('fash');
var verror = require('verror');

var schema = require('./schema/index');


var READ_ONLY = 'ro';


function Ring(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.ring, 'options.ring');

    var self = this;

    this.log_ = options.log;
    this.ring_ = options.ring;
    this.ro_ = false;

    // check whether this has has r/o nodes
    //var vnodeMap = self.ring_.getAllVnodes();
    //var vnodeKeys = Object.keys(vnodeMap);
    //for (var i = 0; i < vnodeKeys.length; i++) {
        //var vnode = vnodeKeys[i];
        //if (vnodeMap[vnode].data === READ_ONLY) {
            //this.ro_ = true;
            //break;
        //}
    //}

    self.log_.info({ro: this.ro_}, 'Ring.new: instantiated Ring');
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

    self.ring_.getNode(tkey, function (err, hashedNode) {
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

    self.ring_.getNode(key, function (err, hashedNode) {
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

    self.ring_.getPnodes(function (err, pnodes) {
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

    options.log = options.log.child({component: 'ring'});

    fash.deserialize({
        log: options.log,
        topology: options.topology,
        backend: fash.BACKEND.LEVEL_DB,
        location: '/var/tmp/yunong'
    }, function (err, ring) {
        if (err) {
            return callback(err);
        }
        options.ring = ring;
        return callback(null, new Ring(options));
    });
}
