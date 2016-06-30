'use strict';

let util = require('util');
let debug = require('debug')('test');
let _ = require('lodash');
let client = require('redis-hook')({
    host: 'localhost'
});
let shortid = require('shortid');
let scan = require('../../lib')(client);

module.exports = function(test, Promise) {

    Promise.config({
        longStackTraces: true
    });

    // Create a bunch of values in a sorted set.
    //
    let batch = [];
    let total = 10;
    let testKey = 'test:key:for:scan';

    while(total--) {
        batch.push(total, shortid.generate())
    }

    return client
    .zaddAsync(testKey, batch)
    .then(() => scan((acc, scan) => {

        let len = scan.length;

        while(len--) {
            !(len%2) && acc.push(`${len} -> ${scan[len]}`);
        }

        return acc;

    }, [], 'zscan', { key: testKey }))

    .then((acc) => {

        test.ok(acc.length === 10, 'Correctly scanning collection');

        return scan((acc, scan) => {

            let len = scan.length;

            while (len--) {

                if (len === 5) {
                    return false;
                }

                !(len % 2) && acc.push(`${len} -> ${scan[len]}`);
            }

            return acc;

        }, [], 'zscan', { key: testKey });
    })
    .then((acc) => {

        test.ok(acc.length === 7, 'Reducer can request premature end to scan')
    })

    .catch((err) => {
        debug('error', err.message)
    })

    .finally(() => {
        client.del(testKey);
    })
}
