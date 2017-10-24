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
var VError = verror.VError;

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

function InvocationError() {
        VError.apply(this, arguments);
        this.name = this.constructor.name;
}
util.inherits(InvocationError, VError);

function ReadOnlyError(cause) {
        if (arguments.length === 0) {
                cause = {};
        }
        WError.call(this, cause, 'some vnodes are in read-only mode');
        this.name = this.constructor.name;
}
util.inherits(ReadOnlyError, WError);



///--- Exports

module.exports = {
    InvocationError: InvocationError,
    ReadOnlyError: ReadOnlyError
};
