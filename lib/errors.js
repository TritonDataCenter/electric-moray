/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var util = require('util');
var VError = require('verror').VError;

function InvocationError() {
    VError.apply(this, arguments);
    this.name = this.constructor.name;
}
util.inherits(InvocationError, VError);

function ReadOnlyError(pnode) {
    if (!pnode) {
        VError.call(this, {}, 'some vnodes are in read-only mode');
    } else {
        VError.call(this, 'pnode "%s" is in read-only mode', pnode);
    }
    this.name = this.constructor.name;
}
util.inherits(ReadOnlyError, VError);

module.exports = {
    InvocationError: InvocationError,
    ReadOnlyError: ReadOnlyError
};
