/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var server = require('./server');
var status_server = require('./status_server');
var bucket_server = require('./bucket_server');
var bucket_status_server = require('./bucket_status_server');

module.exports = {
    createServer: server.createServer,
    createStatusServer: status_server.createStatusServer,
    createBucketServer: bucket_server.createServer,
    createBucketStatusServer: bucket_status_server.createStatusServer
};
