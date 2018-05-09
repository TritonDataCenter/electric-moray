/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016, Joyent, Inc.
 */

/*
 * Mock moray object for unit tests
 */

var assert = require('assert-plus');
var crc = require('crc');
var clone = require('clone');
var EventEmitter = require('events').EventEmitter;
var ldapjs = require('ldapjs');
var util = require('util');
var verror = require('verror');



// --- Globals



var BUCKETS = {
    'ufds_o_smartdc': {}
};
var BUCKET_VALUES = {
    'ufds_o_smartdc': {}
};
var LAST_MORAY_ERROR;
var MORAY_ERRORS = {};



// --- Internal


function compareTo(a, b) {
    if (typeof (a) === 'number') {
        return a - b;
    } else {
        if (a < b) {
            return -1;
        } else if (a > b) {
            return 1;
        } else {
            return 0;
        }
    }
}

/**
 * If there's an error in MORAY_ERRORS for the given operation, return it.
 */
function getNextMorayError(op, details) {
    if (!MORAY_ERRORS.hasOwnProperty(op) ||
        typeof (MORAY_ERRORS[op]) !== 'object' ||
        MORAY_ERRORS[op].length === 0) {
        return;
    }

    var morayErr = MORAY_ERRORS[op].shift();

    // Allow passing null in the array to allow interleaving successes and
    // errors
    if (morayErr === null) {
        return;
    }

    LAST_MORAY_ERROR = clone(details);
    LAST_MORAY_ERROR.op = op;
    LAST_MORAY_ERROR.msg = morayErr.message;

    return morayErr;
}


/**
 * Returns a not found error for the bucket
 */
function bucketNotFoundErr(bucket) {
    var err = new verror.VError('bucket "%s" does not exist', bucket);
    err.name = 'BucketNotFoundError';
    return err;
}


/**
 * Do etag checks on a record
 */
function checkEtag(opts, bucket, key, batch) {
    if (!opts || !opts.hasOwnProperty('etag')) {
        return;
    }

    var errOpts = {};
    if (batch) {
        errOpts = {
            context: {
                key: key,
                bucket: bucket
            }
        };
    }

    if (BUCKET_VALUES[bucket].hasOwnProperty(key)) {
        if (opts.etag === null) {
            throw etagConflictErr(util.format('key "%s" already exists', key),
                errOpts);
        }

        var obj = BUCKET_VALUES[bucket][key];
        if (opts.etag != obj._etag) {
            throw etagConflictErr(
                util.format('wanted to put etag "%s", but object has etag "%s"',
                    opts.etag, obj._etag), errOpts);
        }
    }
}


/**
 * Generates an etag for an object
 */
function eTag(val) {
    return (crc.hex32(crc.crc32(JSON.stringify(val))));
}


/**
 * Returns a not found error for the bucket
 */
function etagConflictErr(msg, otherOpts) {
    var err = new verror.VError(msg);
    err.name = 'EtagConflictError';

    if (otherOpts) {
        for (var o in otherOpts) {
            err[o] = otherOpts[o];
        }
    }

    return err;
}


function matchObj(filter, origObj) {
    // The LDAP matching function .matches() assumes that the
    // values are strings, so stringify properties so that matches
    // work correctly.  The exception is arrays - it's able to walk
    // an array and match each element individually.
    var obj = {};
    for (var k in origObj.value) {
        var val = origObj.value[k];
        if (util.isArray(val)) {
            obj[k] = clone(origObj.value[k]);
        } else {
            obj[k] = origObj.value[k].toString();
        }
    }

    if (filter.matches(obj)) {
        return true;
    }

    return false;
}


/**
 * Returns an object not found error
 */
function objectNotFoundErr(key) {
    var err = new verror.VError('key "%s" does not exist', key);
    err.name = 'ObjectNotFoundError';
    return err;
}



// --- Fake moray object



function FakeMoray(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    this.log = opts.log.child({ component: 'mock-moray' });
    this._version = opts.version || 2;
    EventEmitter.call(this);
}

util.inherits(FakeMoray, EventEmitter);


FakeMoray.prototype._del = function _del(bucket, key) {
    var err = getNextMorayError('delObject', { bucket: bucket, key: key });
    if (err) {
        throw err;
    }

    if (!BUCKET_VALUES.hasOwnProperty(bucket)) {
        throw bucketNotFoundErr(bucket);
    }

    if (!BUCKET_VALUES[bucket].hasOwnProperty(key)) {
        throw objectNotFoundErr(key);
    }

    delete BUCKET_VALUES[bucket][key];
};


FakeMoray.prototype._put = function _store(bucket, key, val) {
    var obj = {
        _etag: eTag(val),
        bucket: bucket,
        key: key,
        value: clone(val)
    };

    this.log.trace({ bucket: bucket, obj: obj }, '_put object');
    BUCKET_VALUES[bucket][key] = obj;
};


FakeMoray.prototype._updateObjects =
    function _updateObjects(bucket, fields, filter) {
    assert.object(fields, 'fields');
    assert.string(filter, 'filter');

    // XXX: should throw if trying to set a non-indexed field

    var filterObj = ldapjs.parseFilter(filter);
    for (var r in BUCKET_VALUES[bucket]) {
        if (matchObj(filterObj, BUCKET_VALUES[bucket][r])) {
            for (var nk in fields) {
                BUCKET_VALUES[bucket][r].value[nk] = fields[nk];
            }
        }
    }
};




FakeMoray.prototype.batch = function batch(data, callback) {
    assert.arrayOfObject(data, 'data');

    var err = getNextMorayError('batch', { batch: data });
    if (err) {
        return callback(err);
    }

    for (var b in data) {
        var item = data[b];
        assert.string(item.bucket, 'item.bucket');
        assert.string(item.operation, 'item.operation');

        var knownOp = false;
        ['delete', 'put', 'update'].forEach(function (opt) {
            if (item.operation == opt) {
                knownOp = true;
            }
        });

        if (!knownOp) {
            throw new verror.VError('Unknown moray operation "%s"',
                item.operation);
        }

        if (item.operation !== 'update') {
            assert.string(item.key, 'item.key');
        }

        if (item.operation === 'put') {
            assert.object(item.value, 'item.value');
            if (!BUCKET_VALUES.hasOwnProperty(item.bucket)) {
                return callback(bucketNotFoundErr(item.bucket));
            }

            try {
                checkEtag(item.options, item.bucket, item.key, true);
            } catch (eTagErr) {
                return callback(eTagErr);
            }

            this._put(item.bucket, item.key, item.value);
        }

        if (item.operation === 'delete') {
            try {
                this._del(item.bucket, item.key);
            } catch (err2) {
                return callback(err2);
            }
        }

        if (item.operation === 'update') {
            if (!BUCKET_VALUES.hasOwnProperty(item.bucket)) {
                return callback(bucketNotFoundErr(item.bucket));
            }

            this._updateObjects(item.bucket, item.fields, item.filter);
        }
    }

    return callback();
};


FakeMoray.prototype.close = function morayClose() {
    return;
};


FakeMoray.prototype.createBucket =
    function createBucket(bucket, schema, opts, callback) {

    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    var err = getNextMorayError('createBucket', { bucket: bucket });
    if (err) {
        return callback(err);
    }

    BUCKETS[bucket] = clone(schema);
    BUCKET_VALUES[bucket] = {};
    return callback();
};


FakeMoray.prototype.delBucket = function delBucket(bucket, opts, callback) {
    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    var err = getNextMorayError('delBucket', { bucket: bucket });
    if (err) {
        return callback(err);
    }

    if (!BUCKET_VALUES.hasOwnProperty(bucket)) {
        return callback(bucketNotFoundErr(bucket));
    }

    delete BUCKET_VALUES[bucket];
    return callback();
};


FakeMoray.prototype.delObject = function delObject(bucket, key, opts, callback) {
    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    try {
        this._del(bucket, key);
        return callback();
    } catch (err) {
        return callback(err);
    }
};


FakeMoray.prototype.findObjects = function findObjects(bucket, filter, opts) {
    if (opts === undefined) {
        opts = {};
    }

    var res = new EventEmitter();
    var filterObj = ldapjs.parseFilter(filter);
    var limit = opts.limit || 1000;
    var offset = opts.offset || 0;
    var i;


    process.nextTick(function () {
        var err = getNextMorayError('findObjects',
            { bucket: bucket, filter: filter });
        if (err) {
            res.emit('error', err);
            return;
        }

        if (!BUCKET_VALUES.hasOwnProperty(bucket)) {
            res.emit('error', bucketNotFoundErr(bucket));
            return;
        }

        // Whenever we call findObjects, it's either unsorted or sorted by ASC,
        // so just sort them ASC every time
        var keys = Object.keys(BUCKET_VALUES[bucket]).sort(compareTo);
        i = 0;
        keys.forEach(function (r) {
            var val = BUCKET_VALUES[bucket][r];
            if (matchObj(filterObj, val)) {
                if (i >= offset && i < offset + limit) {
                    res.emit('record', clone(val));
                }
                i++;
            }
        });

        res.emit('end');
    });

    return res;
};


FakeMoray.prototype.deleteMany =
    function deleteMany(bucket, filter, opts, callback) {
    var filterObj = ldapjs.parseFilter(filter);

    if (callback === undefined) {
        callback = opts;
        opts = {};
    }

    if (opts === undefined) {
        opts = {};
    }

    var limit = opts.limit || 1000;
    var offset = opts.offset || 0;

    var err = getNextMorayError('deleteMany');
    if (err) {
        return callback(err);
    }

    if (!BUCKETS.hasOwnProperty(bucket)) {
        return callback(bucketNotFoundErr(bucket));
    }

    // Whenever we call findObjects, it's either unsorted or sorted by ASC,
    // so just sort them ASC every time
    var keys = Object.keys(BUCKET_VALUES[bucket]).sort(compareTo);
    var i = 0;
    keys.forEach(function (r) {
        var val = BUCKET_VALUES[bucket][r];
        if (matchObj(filterObj, val)) {
            if (i >= offset && i < offset + limit) {
                delete BUCKET_VALUES[bucket][r];
            }
            i++;
        }
    });

    return callback();
};


FakeMoray.prototype.getBucket = function getBucket(bucket, opts, callback) {
    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    var err = getNextMorayError('getBucket', { bucket: bucket });
    if (err) {
        return callback(err);
    }

    if (!BUCKETS.hasOwnProperty(bucket)) {
        return callback(bucketNotFoundErr(bucket));
    }

    return callback(null, clone(BUCKETS[bucket]));
};


FakeMoray.prototype.getObject = function getObject(bucket, key, opts, callback) {
    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    var err = getNextMorayError('getObject', { bucket: bucket, key: key });
    if (err) {
        return callback(err);
    }

    if (!BUCKET_VALUES.hasOwnProperty(bucket)) {
        return callback(bucketNotFoundErr(bucket));
    }

    if (!BUCKET_VALUES[bucket].hasOwnProperty(key)) {
        return callback(objectNotFoundErr(key));
    }

    var rec = clone(BUCKET_VALUES[bucket][key]);
    this.log.trace({ bucket: bucket, key: key, rec: rec }, 'got object');
    return callback(null, rec);
};


FakeMoray.prototype.putObject =
    function putObject(bucket, key, value, opts, callback) {
    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    var err = getNextMorayError('putObject',
            { bucket: bucket, key: key, value: value, opts: opts });
    if (err) {
        return callback(err);
    }

    if (!BUCKET_VALUES.hasOwnProperty(bucket)) {
        return callback(bucketNotFoundErr(bucket));
    }

    try {
        checkEtag(opts, bucket, key);
    } catch (eTagErr) {
        return callback(eTagErr);
    }

    this._put(bucket, key, value);
    return callback();
};


FakeMoray.prototype.reindexObjects =
        function reindexObjects(bucket, count, opts, callback) {
    return callback(null, { processed: 0 });
};


FakeMoray.prototype.sql = function sql(str) {
    throw new Error('.sql() method not implemented!');
};


FakeMoray.prototype.updateBucket =
    function updateBucket(bucket, schema, opts, callback) {

    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }


    BUCKETS[bucket] = clone(schema);
    return callback();
};


FakeMoray.prototype.putBucket = function putBucket(b, schema, callback) {
    var self = this;
    var err = getNextMorayError('putBucket');
    if (err) {
        return callback(err);
    }

    self.getBucket(b, function (err2, bucket) {
        if (err2) {
            if (err2.name === 'BucketNotFoundError') {
                return self.createBucket(b, schema, callback);
            } else {
                return callback(err2);
            }
        }

        var v = bucket.options.version;
        var v2 = (schema.options || {}).version || 0;
        if (v !== 0 && v === v2) {
            return callback();
        } else {
            return self.updateBucket(b, schema, callback);
        }
    });
};


FakeMoray.prototype.updateObjects =
    function updateObjects(bucket, fields, filter, opts, callback) {
    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    assert.object(bucket, 'bucket');

    if (!BUCKET_VALUES.hasOwnProperty(bucket)) {
        return callback(bucketNotFoundErr(bucket));
    }

    this._updateObjects(bucket, fields, filter);
    return callback();
};



FakeMoray.prototype.version = function morayVersion(opts, callback) {
    var self = this;
    if (typeof (opts) === 'function') {
        callback = opts;
    }
    setImmediate(function () {
        return callback(self._version);
    });
};



// --- Exports



function createClient(opts) {
    var client = new FakeMoray(opts);
    process.nextTick(function () {
        client.emit('connect');
    });

    return client;
}



module.exports = {
    FakeMoray: FakeMoray,
    createClient: createClient
};


Object.defineProperty(module.exports, '_bucketSchemas', {
    get: function () { return BUCKETS; }
});


Object.defineProperty(module.exports, '_buckets', {
    get: function () { return BUCKET_VALUES; }
});


Object.defineProperty(module.exports, '_errors', {
    get: function () { return MORAY_ERRORS; },
    set: function (obj) { MORAY_ERRORS = obj; }
});


Object.defineProperty(module.exports, '_lastError', {
    get: function () { return LAST_MORAY_ERROR; }
});
