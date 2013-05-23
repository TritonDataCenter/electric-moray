// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fash = require('fash');
var restify = require('restify');

var schema = require('./schema/index');

function Ring(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.ring, 'options.ring');

    var self = this;

    this.log_ = options.log;
    this.ring_ = options.ring;

    self.log_.info('Ring.new: instantiated Ring');
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
 *
 * @return {Object} hashedNode The hashed node.
 */
Ring.prototype.getNode = function getNode(bucket, key) {
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

    var hashedNode = self.ring_.getNode(tkey);

    log.debug({
        bucket: bucket,
        key: key,
        tkey: tkey,
        hashedNode: hashedNode
    }, 'Ring.getNode: exiting');

    return hashedNode;
};

/**
 * Gets the hashed node given only a key. The key is not transformed in anyway.
 * @param {String} key The key.
 *
 * @return {Object} hashedNode The hashed node.
 */
Ring.prototype.getNodeNoSchema = function getNodeNoSchema(key) {
    var self = this;
    var log = self.log_;

    log.debug({
        key: key
    }, 'Ring.getNode: entered');

    var hashedNode = self.ring_.getNode(key);

    log.debug({
        key: key,
        hashedNode: hashedNode
    }, 'Ring.getNode: exiting');

    return hashedNode;
};

Ring.prototype.getPnodes = function getPnodes() {
    var self = this;
    var log = self.log_;

    log.debug('Ring.getPnodes: entered');

    var pnodes = [];

    self.ring_.pnodes_.forEach(function(pnode) {
        pnodes.push(pnode);
    });
    log.debug({pnodes: pnodes}, 'Ring.getPnodes: exiting');

    return pnodes;
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

function deserializeRing(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.topology, 'options.topology');

    options.log = options.log.child({component: 'ring'});

    options.ring = fash.deserialize({
        log: options.log,
        topology: options.topology
    });

    return new Ring(options);
}
