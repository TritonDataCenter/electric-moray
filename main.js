/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var bsyslog = require('bunyan-syslog');
var bunyan = require('bunyan');
var clone = require('clone');
var fs = require('fs');
var jsprim = require('jsprim');
var getopt = require('posix-getopt');
var VError = require('verror');
var extend = require('xtend');

var app = require('./lib');

var MIN_PORT = 1;
var MAX_PORT = 65535;

var DEFAULTS = {
    file: process.cwd() + '/etc/config.json',
    port: 2020,
    monitorPort: 3020,
    statusPort: 4020,
    bindip: '0.0.0.0'
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


function parsePort(str) {
    var port = jsprim.parseInteger(str);

    if (port instanceof Error) {
        LOG.fatal({ port: str }, 'Invalid port - failed to parse');
        throw new VError(port, 'Invalid port %j', str);
    }

    if (port < MIN_PORT || port > MAX_PORT) {
        LOG.fatal({ port: str }, 'Invalid port - out of range');
        throw new VError(port, 'Invalid port %j, should be in range %d-%d',
                str, MIN_PORT, MAX_PORT);
    }

    return port;
}


function parseOptions() {
    var option;
    var opts = {};
    var parser = new getopt.BasicParser('cvf:r:p:k:s:', process.argv);

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
                opts.port = parsePort(option.optarg);
                break;
            case 'k':
                opts.monitorPort = parsePort(option.optarg);
                break;
            case 's':
                opts.statusPort = parsePort(option.optarg);
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

    app.createServer(opts, function (err, res) {
        if (err) {
            LOG.fatal(err, 'startup failed');
            process.exit(1);
        }

        assert.object(res, 'res');
        assert.object(res.ring, 'res.ring');
        assert.arrayOfString(res.clientList, 'res.clientList');

        app.createStatusServer({
            log: LOG.child({ component: 'statusServer' }),
            ring: res.ring,
            clientList: res.clientList,
            indexShards: opts.ringCfg.indexShards,
            port: opts.statusPort
        }, function (err2) {
            if (err2) {
                LOG.fatal(err2, 'status server startup failed');
                process.exit(1);
            }
        });
    });
}


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
})();
