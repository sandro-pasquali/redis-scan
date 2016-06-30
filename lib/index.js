'use strict';

let Promise = require('bluebird');
let _ = require('lodash');

module.exports = (client) => {

    // Functionally identical to running a reduce function, where #reducer is
    // expected to update and return #acc(umulator) on each scan result.
    //
    function scan(reducer, acc, scanMethod, opts) {

        opts = opts || {};

        return new Promise(resolve => {

            if(!_.isFunction(reducer)) {
                throw new Error('#redis-scan requires a Function as first argument');
            }

            if(!_.isString(scanMethod)) {
                throw new Error('#redis-scan requires a scan method');
            }

            scanMethod = scanMethod.toLowerCase();

            if(!~['zscan','sscan','hscan','scan'].indexOf(scanMethod)) {
                throw new Error(`Invalid scan method sent to #redis-scan: ${scanMethod}`)
            }

            let args = ['+'];

            scanMethod !== 'scan' && args.unshift(opts.key || '');
            args = args.concat(!!opts.match ? ['MATCH', opts.match] : '');
            args = args.concat(!!opts.count ? ['COUNT', +opts.count] : '');

            args = _.compact(args);

            (function scanner(cursor) {

                args[scanMethod === 'scan' ? 0 : 1] = +cursor;

                client[scanMethod](args, (err, scn) => {

                    if(err) {
                        throw new Error(err.message);
                    }

                    let res = reducer(acc, scn[1]);

                    acc = res === false ? acc : res;

                    // Terminate the scan if reducer returns false, or nothing left.
                    //
                    if(res === false || +scn[0] === 0) {
                        return resolve(acc);
                    }

                    return scanner(scn[0]);
                })
            })(0);
        });
    }

    return scan;
};