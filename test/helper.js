/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var deepEqual = require('deep-equal');
var fast = require('fast');
var moray = require('moray'); // client
var net = require('net');



///--- Exports

module.exports = {

        after: function after(teardown) {
                module.parent.exports.tearDown = function _teardown(callback) {
                        try {
                                teardown.call(this, callback);
                        } catch (e) {
                                console.error('after:\n' + e.stack);
                                process.exit(1);
                        }
                };
        },

        before: function before(setup) {
                module.parent.exports.setUp = function _setup(callback) {
                        try {
                                setup.call(this, callback);
                        } catch (e) {
                                console.error('before:\n' + e.stack);
                                process.exit(1);
                        }
                };
        },

        test: function test(name, tester) {
                module.parent.exports[name] = function _(t) {
                        var _done = false;
                        t.end = function end() {
                                if (!_done) {
                                        _done = true;
                                        t.done();
                                }
                        };
                        t.notOk = function notOk(ok, message) {
                                return (t.ok(!ok, message));
                        };

                        tester.call(this, t);
                };
        },

        createLogger: function createLogger(name, stream) {
                var log = bunyan.createLogger({
                        level: (process.env.LOG_LEVEL || 'warn'),
                        name: name || process.argv[1],
                        stream: stream || process.stdout,
                        src: true,
                        serializers: bunyan.stdSerializers
                });
                return (log);
        },

        createClient: function createClient() {
                var client = moray.createClient({
                        unwrapErrors: true,
                        host: (process.env.MORAY_IP || '127.0.0.1'),
                        port: (parseInt(process.env.MORAY_PORT, 10) || 2020),
                        log: module.exports.createLogger()
                });
                return (client);
        },

        makeFastRequest: function makeFastRequest(opts, cb) {
                var host, port;
                host = (process.env.MORAY_IP || '127.0.0.1');
                port = (parseInt(process.env.MORAY_PORT, 10) || 2020);

                var socket = net.connect(port, host);

                socket.on('error', cb);

                socket.on('connect', function () {
                    socket.removeListener('error', cb);
                    var client = new fast.FastClient({
                        log: opts.log,
                        nRecentRequests: 100,
                        transport: socket
                    });

                    client.rpcBufferAndCallback(opts.call,
                        function (err, data, ndata) {
                        client.detach();
                        socket.destroy();
                        cb(err, data, ndata);
                    });
                });
        }

};
