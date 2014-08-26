/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var fs = require('fs');
var util = require('util');

var assert = require('assert-plus');
var verror = require('verror');




///--- Globals

var WError = verror.WError;

var slice = Function.prototype.call.bind(Array.prototype.slice);



///--- Helpers

function ISODateString(d) {
    function pad(n) {
        return n < 10 ? '0' + n : n;
    }

    if (typeof (d) === 'string')
        d = new Date(d);

    return d.getUTCFullYear() + '-'
        + pad(d.getUTCMonth()+1) + '-'
        + pad(d.getUTCDate()) + 'T'
        + pad(d.getUTCHours()) + ':'
        + pad(d.getUTCMinutes()) + ':'
        + pad(d.getUTCSeconds()) + 'Z';
}



///--- Errors

function ReadOnlyError(cause) {
        if (arguments.length === 0) {
                cause = {};
        }
        WError.call(this, cause, 'some vnodes are in read-only mode');
        this.name = this.constructor.name;
}
util.inherits(ReadOnlyError, WError);



///--- Exports

// Auto export all Errors defined in this file
fs.readFileSync(__filename, 'utf8').split('\n').forEach(function (l) {
        /* JSSTYLED */
        var match = /^function\s+(\w+)\(.*/.exec(l);
        if (match !== null && Array.isArray(match) && match.length > 1) {
                if (/\w+Error$/.test(match[1])) {
                        module.exports[match[1]] = eval(match[1]);
                }
        }
});


Object.keys(module.exports).forEach(function (k) {
        global[k] = module.exports[k];
});
