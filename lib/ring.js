// Copyright (c) 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var fash = require('fash');
var restify = require('restify');


function Ring(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.ring, 'options.ring');
    //assert.object(options.restify, 'options.restify');

    var self = this;

    this.log_ = options.log;
    this.ring_ = options.ring;

    //options.restify.log = self.log_.childLogger({component: 'restify'});
    //this.server_ = restify.createServer(options.restify);

    //self.server_.get('/ring/topology', getTopology);
    //self.server_.get('/ring/version', getVersion);

    self.log_.info('Ring.new: instantiated Ring');
}


module.exports = {
    createRing: createRing,
    deserializeRing: deserializeRing
};


function createRing(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.arrayOfString(options.pnodes, 'options.pnodes');
    assert.number(options.vnodes, 'options.vnodes');
    assert.optionalObject(options.algorithm, 'options.algorithm');

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
    assert.object(options.topology, 'options.topology');

    options.log = options.log.child({component: 'ring'});

    options.ring = fash.deserialize({
        log: options.log,
        topology: options.topology
    });

    return new Ring(options);
}

Ring.prototype.getNode = function getNode(key) {
    var self = this;
    var log = self.log_;

    log.debug({
        key: key
    }, 'Ring.getNode: entering');

    var hashedNode = self.ring_.getNode(key);

    log.debug({
        key: key,
        hashedNode: hashedNode
    }, 'Ring.getNode: exiting');

    return hashedNode;
};
