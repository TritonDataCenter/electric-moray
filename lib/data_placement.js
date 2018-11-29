/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var crypto = require('crypto');
var fash = require('fash');
var vasync = require('vasync');
var schema = require('./schema/index');


function DataDirector(options, cb) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.string(options.dataPlacementServerUrl,
        'options.dataPlacementServerUrl');
    assert.func(cb, 'callback');

    var self = this;

    this.version = null;
    this.pnodes = [];
    this.log_ = options.log;

    vasync.pipeline({
        arg: self,
        funcs: [
            getDirectorVersion,
            getPlacementData
        ]
    }, function (err, _) {
        if (err) {
            return (cb(err));
        }
        self.log_.info('dataDirector.new: initialized new data director');
        return (cb(null, self));
    });
}

//TODO: Eventually this should call out to data placement service
function getDirectorVersion(self, callback) {
    self.dataPlacement.version = '1.0.0';
    return callback(null, true);
}

//TODO: Eventually this should call out to data placement service
function getPlacementData(self, callback) {
    if (self.dataPlacement.version === '1.0.0') {
        //TODO: Eventually this should call out to a separate function per version
        var ring =  {
            "algorithm_": {
                "NAME": "sha256",
                "MAX": "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
                "VNODE_HASH_INTERVAL": "1fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
            },
            "algorithmMax_": null,
            "vnodeCount_": 8,
            "vnodesBignum_": null,
            "VNODE_HASH_INTERVAL": null,
            "pnodes_": [
                "1.moray",
                "2.moray"
            ],
            "pnodeToVnodeMap_": {
                "1.moray": {
                    "0": 1,
                    "2": 1,
                    "4": 1,
                    "6": 1
                },
                "2.moray": {
                    "1": 1,
                    "3": 1,
                    "5": 1,
                    "7": 1
                }
            },
            "vnodeToPnodeMap_": {
                "0": {
                    "pnode": "1.moray"
                },
                "1": {
                    "pnode": "2.moray"
                },
                "2": {
                    "pnode": "1.moray"
                },
                "3": {
                    "pnode": "2.moray"
                },
                "4": {
                    "pnode": "1.moray"
                },
                "5": {
                    "pnode": "2.moray"
                },
                "6": {
                    "pnode": "1.moray"
                },
                "7": {
                    "pnode": "2.moray"
                }
            },
            "vnodeData_": [],
            "msg": "",
            "time": "2018-11-16T22:03:53.367Z",
            "version": 0
        };
        self.dataPlacement.ring = ring;

        return callback(null, ring);
    } else {
        var err = new Error('Invalid data placement version: ' +
            self.dataPlacement.version);
        return callback(err);
    }
}

// //TODO: Eventually this should call out to data placement service
// function getVnodePnodeMapping(options, callback) {
//     var mapping = { 1: "1.moray",
//                     2: "2.moray"
//                   };
//     return callback(null, mapping);
// }


///--- API

/**
 * Gets the hashed pnode for an object given an owner, bucket, and key.
 * @param {String} bucket The bucket this key belongs to, if a schema exists
 * for this bucket, the key is transformed.
 * @param {String} key The key.
 * @param {Function} callback The callback of the type f(err, hashedNode).
 *
 */
DataDirector.prototype.getObjectLocation = function getObjectLocation(version, owner, bucket, key, callback) {
    var self = this;
    var log = self.log_;

    log.debug({
        bucket: bucket,
        key: key
    }, 'DataDirector.getNode: entered');

    var tkey = owner + ':' + bucket + ':' + key;

    log.debug({
        key: key,
        tkey: tkey
    }, 'DataDirector.getNode: key transformed');

    var value = crypto.createHash(this.algorithm_.NAME).
        update(tkey).digest('hex');
    // find the node that corresponds to this hash.
    var vnodeHashinterval = this.dataPlacement.ring.algorithm_.VNODE_HASH_INTERVAL;
    var vnode = parseInt(bignum(options.hash, 16).
                         div(vnodeHashInterval), 10);
    var pnode = this.dataPlacement.ring.vnodeToPnodeMap_[vnode].pnode;
    var data = this.dataPlacement.ring.pnodeToVnodeMap_[pnode][vnode];
    // dtrace._fash_probes['getnode-done'].fire(function () {
    //     return ([null, key, value, pnode, vnode, data]);
    // });

    return cb(null, {vnode: vnode, pnode: pnode, data: data});

    // self.chash_.getNode(tkey, function (err, hashedNode) {
    //     log.debug({
    //         err: err,
    //         bucket: bucket,
    //         key: key,
    //         tkey: tkey,
    //         hashedNode: hashedNode
    //     }, 'DataDirector.getNode: exiting');

    //     return callback(err, hashedNode);
    // });
};

    // dtrace._fash_probes['getnode-start'].fire(function () {
    //     return ([key]);
    // });
    // assert.optionalFunc(cb, 'callback');



};

/**
 * Gets the hashed pnode for a bucket given an owner and bucket.
 * @param {String} bucket The bucket this key belongs to, if a schema exists
 * for this bucket, the key is transformed.
 * @param {String} key The key.
 * @param {Function} callback The callback of the type f(err, hashedNode).
 *
 */
DataDirector.prototype.getBucketLocation = function getBucketLocation(version, owner, bucket, callback) {
    var self = this;
    var log = self.log_;

    log.debug({
        bucket: bucket,
        key: key
    }, 'DataDirector.getNode: entered');

    var tkey = owner + ':' + bucket;

    log.debug({
        key: key,
        tkey: tkey
    }, 'DataDirector.getNode: key transformed');

    var value = crypto.createHash(this.algorithm_.NAME).
        update(tkey).digest('hex');
    // find the node that corresponds to this hash.
    var vnodeHashinterval = this.dataPlacement.ring.algorithm_.VNODE_HASH_INTERVAL;
    var vnode = parseInt(bignum(options.hash, 16).
                         div(vnodeHashInterval), 10);
    var pnode = this.dataPlacement.ring.vnodeToPnodeMap_[vnode].pnode;
    var data = this.dataPlacement.ring.pnodeToVnodeMap_[pnode][vnode];
    // dtrace._fash_probes['getnode-done'].fire(function () {
    //     return ([null, key, value, pnode, vnode, data]);
    // });

    return cb(null, {pnode: pnode, data: data});

    // self.chash_.getNode(tkey, function (err, hashedNode) {
    //     log.debug({
    //         err: err,
    //         bucket: bucket,
    //         key: key,
    //         tkey: tkey,
    //         hashedNode: hashedNode
    //     }, 'DataDirector.getNode: exiting');

    //     return callback(err, hashedNode);
    // });
};


function findVnode(options) {
    assert.object(options, 'options');
    assert.object(options.vnodeHashInterval, 'options.vnodeHashinterval');
    assert.string(options.hash, 'options.hash');
    return parseInt(bignum(options.hash, 16).
        div(options.vnodeHashInterval), 10);
}

/**
 * Gets the hashed node given a series of batch operations.
 *
 * @param {Array} requests An array of {bucket,key} objects which must all
 * transform to the same key to determine which node a series of operations
 * will performed on.
 * @param {Function} callback The callback of the type f(err, hashedNode).
 */
// Ring.prototype.getNodeBatch = function getNodeBatch(origRequests, callback) {
//     var self = this;
//     var log = self.log_;
//     var requests = origRequests.slice();

//     log.debug({
//         requests: requests
//     }, 'Ring.getNodeBatch: entered');

//     var request = requests.shift();
//     var tkey = schema.transformKey(request.bucket, request.key);

//     for (var i = 0; i < requests.length; i++) {
//         var currKey = schema.transformKey(requests[i].bucket, requests[i].key);
//         if (tkey !== currKey) {
//             setImmediate(callback,
//                 new Error('all requests must transform to the same key'));
//             return;
//         }
//     }

//     log.debug({
//         requests: requests,
//         tkey: tkey
//     }, 'Ring.getNodeBatch: key transformed');

//     self.chash_.getNode(tkey, function (err, hashedNode) {
//         log.debug({
//             err: err,
//             requests: requests,
//             tkey: tkey,
//             hashedNode: hashedNode
//         }, 'Ring.getNodeBatch: exiting');

//         callback(err, hashedNode);
//     });
// };

// /**
//  * Gets the hashed node given only a key. The key is not transformed in anyway.
//  * @param {String} key The key.
//  * @param {Function} callback The callback of the type f(err, hashedNode).
//  *
//  */
// Ring.prototype.getNodeNoSchema = function getNodeNoSchema(key, callback) {
//     var self = this;
//     var log = self.log_;

//     log.debug({
//         key: key
//     }, 'Ring.getNode: entered');

//     self.chash_.getNode(key, function (err, hashedNode) {
//         log.debug({
//             err: err,
//             key: key,
//             hashedNode: hashedNode
//         }, 'Ring.getNode: exiting');

//         return callback(err, hashedNode);
//     });
// };

Ring.prototype.getPnodes = function getPnodes(callback) {
    var self = this;
    var log = self.log_;

    log.debug('DataDirectory.getPnodes: entered');

    if (self.dataPlacement.version === '1.0.0') {
        return (self.ring.pnodes_);
    } else {
        return ([]);
    }
};



///--- Privates


// function loadRing(options, callback) {
//     assert.object(options, 'options');
//     assert.object(options.log, 'options.log');
//     assert.string(options.location, 'options.location');
//     assert.object(options.leveldbCfg, 'options.leveldbCfg');
//     assert.func(callback, 'callback');

//     options.log = options.log.child({component: 'ring'});

//     // Loading the db from disk, so of course we want to turn this error off
//     var leveldbCfg = {
//         errorIfExists: false
//     };

//     options.ring = fash.load({
//         log: options.log,
//         backend: fash.BACKEND.LEVEL_DB,
//         location: options.location,
//         leveldbCfg: options.leveldbCfg || leveldbCfg
//     }, function (err, chash) {
//         if (err) {
//             return callback(err);
//         }
//         options.chash = chash;

//         var r = new Ring(options, function (_err) {
//             return callback(err, r);
//         });
//     });
// }

function createDataDirector(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    options.log = options.log.child({component: 'data_placement'});

    return (new DataDirector(options));
}

// function deserializeRing(options, callback) {
//     assert.object(options, 'options');
//     assert.object(options.log, 'options.log');
//     assert.string(options.topology, 'options.topology');
//     assert.string(options.location, 'options.location');

//     options.log = options.log.child({component: 'fash'});

//     fash.deserialize({
//         log: options.log,
//         topology: options.topology,
//         backend: fash.BACKEND.LEVEL_DB,
//         location: options.location
//     }, function (err, chash) {
//         if (err) {
//             return callback(err);
//         }
//         options.chash = chash;

//         var r = new Ring(options, function (_err) {
//             return callback(err, r);
//         });
//     });
// }


///--- Exports

module.exports = {
    createDataDirector: createDataDirector,
    transformKey: transformKey,
    getObjectLocation: getObjectLocation,
    getBucketLocation: getBucketLocation
};
