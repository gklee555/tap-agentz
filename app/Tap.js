#!/usr/bin/env node
'use strict';

const Highland = require('highland');
const minimist = require('minimist');
const fs = require('fs');
const Request = require('request');
const Util = require('util');

const Tap = class Tap {

    constructor(args) {
        if (!args.config) {
            throw 'Usage: tap-agentz --config <config-file>';
        }

        this._config = JSON.parse(fs.readFileSync(args.config, 'utf8'));

        if (!this._config.apiKey) {
            throw 'Config file must have an apiKey';
        }

        this._request = Util.promisify(Request);
    }

    async newAgentzRequest({ apiKey }) {
        const {
            statusCode,
            body: response,
        } = await this._request({
            method: 'POST',
            url: 'https://dev-api.agentz.ai/iam/v1/deploymentreport',
            body: {
                startDate: '2020-01-01',
                endDate: '2020-12-11',
            },
            headers: {
                apiKey,
            },
            json: true,
        });

        if (statusCode !== 200) {

            console.log('Failed to make Agentz API request', {
                statusCode,
                response,
            });

            throw new Error(response && response.message);
        }


        return response;
    }

    async start(outputStream) {
        const messages = await this.streamMessages({ stream: 'TotalNumberofSessions'});

        return new Promise((resolve, reject) => {
            messages
                .on('error', reject)
                .pipe(outputStream)
                .on('done', resolve);
        });
    }

    async streamMessages({ stream } ) {
        return new Highland(
            await this.newAgentzRequest({ apiKey: this._config.apiKey })
        ).map(record =>
            `${this.formatRecord({ stream, record,
                })
            }\n`
        )
    }

    formatRecord({ stream, record }) {
        return JSON.stringify({
            type: 'RECORD',
            stream,
            record,
        })
    }
}

module.exports = Tap;

if (require.main === module) {
    new Tap(minimist(process.argv.slice(2)))
        .start(process.stdout)
        .catch(error => {
            console.error(error.stack || error); // eslint-disable-line no-console
            process.exitCode = 1;
        });
}