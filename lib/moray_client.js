/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');
var url = require('url');

var assert = require('assert-plus');
var jsprim = require('jsprim');
var moray = require('moray');
var verror = require('verror');

/*
 * Create moray clients in order to interact with moray instances.  Available
 * moray clients are listed in the ring configuration in LevelDB, which we
 * access in electric moray via node-fash.
 */
function createClient(options, callback) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.ring, 'options.ring');
    assert.object(options.morayOptions, 'options.morayOptions');
    assert.func(callback, 'options.callback');

    var log = options.log;

    var clientMap = {};
    var clientArray = [];

    options.ring.getPnodes(function (err, pnodes) {
        if (err) {
            throw new verror.VError(err, 'unable to get pnodes');
        }

        assert.arrayOfString(pnodes, 'pnodes');

        pnodes.forEach(function (pnode) {
            var pnodeUrl = url.parse(pnode);
            assert.string(pnodeUrl.port, 'pnodeUrl.port');
            assert.string(pnodeUrl.hostname, 'pnodeUrl.hostname');

            log.info({
                url: pnodeUrl
            }, 'creating moray client');

            var morayargs = jsprim.deepCopy(options.morayOptions);
            if (!morayargs.cueballOptions) {
                morayargs.cueballOptions = {};
            }
            morayargs.unwrapErrors = true;
            morayargs.srvDomain = pnodeUrl.hostname;
            morayargs.cueballOptions.defaultPort = parseInt(pnodeUrl.port, 10);
            morayargs.log = options.log.child({
                component: 'moray-client-' + pnodeUrl.hostname
            });
            morayargs.crc_mode = 1;

            var client = moray.createClient(morayargs);
            clientMap[pnode] = client;
            clientArray.push(client);

            if (clientArray.length === pnodes.length) {
                // write ready cookie when clients have connected
                log.info('all moray clients instantiated writing ready cookie');
                try {
                    fs.writeFileSync('/var/tmp/electric-moray-ready', null);
                } catch (e) {
                    throw new verror.VError(e, 'unable to write ready cookie');
                }
            }
        });

        if (clientArray.length <= 0) {
            throw new verror.VError('No moray clients exist!');
        }

        return callback(null, {
            map: clientMap,
            array: clientArray
        });
    });
}

module.exports = {
    createClient: createClient
};
