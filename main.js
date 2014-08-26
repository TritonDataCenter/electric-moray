/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var fs = require('fs');
var os = require('os');

var assert = require('assert-plus');
var bsyslog = require('bunyan-syslog');
var bunyan = require('bunyan');
var clone = require('clone');
var getopt = require('posix-getopt');
var extend = require('xtend');
var panic = require('panic');

var app = require('./lib');



///--- Globals

var DEFAULTS = {
    file: process.cwd() + '/etc/config.json',
    port: 2020
};
var NAME = 'electric-moray';
var LOG_SERIALIZERS = {
    err: bunyan.stdSerializers.err
};
// We'll replace this with the syslog later, if applicable
var LOG = bunyan.createLogger({
    name: NAME,
    level: (process.env.LOG_LEVEL || 'info'),
    src: true,
    stream: process.stderr,
    serializers: LOG_SERIALIZERS
});
var LOG_LEVEL_OVERRIDE = false;



///--- Internal Functions

function setupLogger(config) {
    var cfg_b = config.bunyan;
    assert.object(cfg_b, 'config.bunyan');
    assert.optionalString(cfg_b.level, 'config.bunyan.level');
    assert.optionalObject(cfg_b.syslog, 'config.bunyan.syslog');

    var level = LOG.level();

    if (cfg_b.syslog && !LOG_LEVEL_OVERRIDE) {
        assert.string(cfg_b.syslog.facility, 'config.bunyan.syslog.facility');
        assert.string(cfg_b.syslog.type, 'config.bunyan.syslog.type');

        var facility = bsyslog.facility[cfg_b.syslog.facility];
        LOG = bunyan.createLogger({
            name: NAME,
            serializers: LOG_SERIALIZERS,
            streams: [ {
                level: level,
                type: 'raw',
                stream: bsyslog.createBunyanStream({
                    name: NAME,
                    facility: facility,
                    host: cfg_b.syslog.host,
                    port: cfg_b.syslog.port,
                    type: cfg_b.syslog.type
                })
            } ]
        });
    }

    if (cfg_b.level && !LOG_LEVEL_OVERRIDE) {
        if (bunyan.resolveLevel(cfg_b.level))
            LOG.level(cfg_b.level);
    }
}


function parseOptions() {
    var option;
    var opts = {};
    var parser = new getopt.BasicParser('cvf:r:p:', process.argv);

    while ((option = parser.getopt()) !== undefined) {
        switch (option.option) {
            case 'c':
                opts.cover = true;
                break;
            case 'f':
                opts.file = option.optarg;
                break;
            case 'r':
                opts.ringLocation = option.optarg;
                break;
            case 'p':
                opts.port = parseInt(option.optarg, 10);
                if (isNaN(opts.port)) {
                    LOG.fatal({
                        port: option.optarg
                    }, 'Invalid port.');
                    process.exit(1);
                }
                break;
            case 'v':
                // Allows us to set -vvv -> this little hackery just ensures
                // that we're never < TRACE
                LOG_LEVEL_OVERRIDE = true;
                LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
                if (LOG.level() <= bunyan.DEBUG)
                    LOG = LOG.child({src: true});
                break;
            default:
                process.exit(1);
                break;
        }
    }

    if (!opts.file) {
        LOG.fatal({ opts: opts }, 'No config file specified.');
        process.exit(1);
    }

    return (opts);
}


function readConfig(options) {
    assert.object(options);

    var cfg;

    try {
        cfg = JSON.parse(fs.readFileSync(options.file, 'utf8'));
    } catch (e) {
        LOG.fatal({
            err: e,
            file: options.file
        }, 'Unable to read/parse configuration file');
        process.exit(1);
    }

    cfg.ringLocation = options.ringLocation;

    return (extend({}, clone(DEFAULTS), cfg, options));
}


function run(options) {
    assert.object(options);

    var opts = clone(options);
    opts.log = LOG;
    opts.name = NAME;

    app.createServer(opts);
}



///--- Mainline

(function main() {
    var options = parseOptions();
    var config = readConfig(options);

    LOG.debug({
        config: config,
        options: options
    }, 'main: options and config parsed');

    setupLogger(config);
    run(config);

    if (options.cover) {
        process.on('SIGUSR2', function () {
            process.exit(0);
        });
    }

    panic.enablePanicOnCrash({
        'skipDump': true,
        'abortOnPanic': true
    });
})();
