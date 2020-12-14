#!/usr/bin/env node
'use strict';

const Highland = require('highland');
const minimist = require('minimist');
const fs = require('fs');
const Request = require('request');
const Util = require('util');

const START_DATE = '2020-08-01'; // Earliest relevant agentz data

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

        this._endDate = new Date().toISOString().split('T')[0]; // Today in ISO 8601
        ;
    }

    toSnakeCase(attr) {
        if (attr === 'TotalNumberofSessions') {
            return 'total_number_of_sessions';
        }

        return attr.replace(/\W+/g, " ")
        .split(/ |\B(?=[A-Z])/)
        .map(word => word.toLowerCase())
        .join('_');
    }

    formatRecord({ record }) {
        let formattedRecord = {};
        for (const [key, val] of Object.entries(record)) {
            formattedRecord[this.toSnakeCase(key)] = val;
        }

        formattedRecord['date_updated'] = this._endDate;

        return formattedRecord;
    };

    formatMessage({ stream, record }) {
        return JSON.stringify({
            type: 'RECORD',
            stream,
            record: this.formatRecord({ record }),
        })
    }

    async agentzRequest({ apiKey }) {
        const {
            statusCode,
            body: response,
        } = await this._request({
            method: 'POST',
            url: 'https://dev-api.agentz.ai/iam/v1/deploymentreport',
            body: {
                startDate: START_DATE,
                endDate: this._endDate,
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

    async streamMessages({ stream } ) {
        return new Highland(
            await this.agentzRequest({ apiKey: this._config.apiKey })
        ).map(record => `${this.formatMessage({ stream, record })}\n`)
    }

    async start(outputStream) {
        const messages = await this.streamMessages({ stream: 'AgentzSessions'});

        return new Promise((resolve, reject) => {
            messages
                .on('error', reject)
                .pipe(outputStream)
                .on('done', resolve);
        });
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