/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var restify = require('restify');

/*
 * Serves HTTP requests for electric-moray process state access.
 */
function createStatusServer(options, callback) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.number(options.port, 'options.port');
    assert.object(options.dataDirectory, 'options.dataDirector');
    assert.arrayOfString(options.clientList, 'options.clientList');
    assert.func(callback, 'callback');

    var opts = {
        log: options.log,
        startTime: (new Date()).toISOString(),
        clientList: options.clientList,
        indexShards: options.indexShards
    };

    /*
     * REST API server to access the status of an electric-moray instance.
     */
    var server = restify.createServer({
        name: 'electric-moray-status'
    });

    server.get('/status', createStatusHandler(opts));

    server.on('error', callback);

    server.listen(options.port, function () {
        opts.log.info('status server listening on port %d', options.port);
        callback();
    });

}

/*
 * Exposes the current configured state of an electric-moray process, providing
 * some details about this process and SMF identity.  This information enables
 * other systems to correlate the output of svcs(1) with this status object.
 */
function createStatusHandler(opts) {
    return (function statusHandler(req, res, next) {
        var body = {
            smf_fmri: process.env.SMF_FMRI || null,
            pid: process.pid,
            start_time: opts.startTime,
            client_list: opts.clientList,
            index_shards: opts.indexShards
        };

        res.send(200, body);
        next();
    });
}

module.exports = {
    createStatusServer: createStatusServer
};
