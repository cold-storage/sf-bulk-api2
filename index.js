#!/usr/bin/env node

'use strict';

const axios = require('axios');
const querystring = require('querystring');

/*

  Provides most of the functionality of the Salesforce Bulk API 2.0.

  https://developer.salesforce.com/docs/atlas.en-us.api_bulk_v2.meta/api_bulk_v2/introduction_bulk_api_2.htm

  Each instance of BulkApi is meant to work with a single Job. To work with a
  different job, create a new BulkApi instance.

*/

class BulkApi {

  constructor(options) {
    /*
      Required options.
    */
    if (!options.url) throw new Error('options.url required');
    if (!options.username) throw new Error('options.username required');
    if (!options.password) throw new Error('options.password required');
    if (!options.token) throw new Error('options.token required');
    if (!options.apiVersion) throw new Error('options.apiVersion required');
    if (!options.consumerKey) throw new Error('options.consumerKey required');
    if (!options.consumerSecret) throw new Error('options.consumerSecret required');
    this.options = options;
    /*
      The following are needed to create a job.
    */
    this.options.object = options.object || null;
    // operation options: insert, upsert, update, delete, query, queryAll
    this.options.operation = options.operation || null;
    // externalIdFieldName only needed for upsert jobs.
    this.options.externalIdFieldName = options.externalIdFieldName || null;
    /*
      The following are set on login/getUrl.
    */
    this.authToken = null;
    this.idUrl = null;
    this.url = null;
    /*
      jobId is set on createJob.
    */
    this.jobId = null;
  }

  async uploadJobData(data) {
    await this.createJob();
    await axios.put(
      `${this.url}/services/data/v${this.options.apiVersion}/jobs/ingest/${this.jobId}/batches`,
      data, {
        headers: {
          authorization: `Bearer ${this.authToken}`,
          'Content-Type': 'text/csv'
        }
      });
  }

  async login() {
    if (!this.authToken) {
      const body = querystring.stringify({
        grant_type: 'password',
        client_id: this.options.consumerKey,
        client_secret: this.options.consumerSecret,
        username: this.options.username,
        password: this.options.password + this.options.token
      });
      const response = await axios.post(
        `${this.options.url}/services/oauth2/token`, body);
      this.authToken = response.data.access_token;
      this.idUrl = response.data.id;
    }
  }

  async getUrl() {
    if (!this.url) {
      await this.login();
      const response = await axios.get(
        this.idUrl, {
          headers: {
            authorization: `Bearer ${this.authToken}`
          }
        });
      this.url = response.data.urls.profile;
      this.url = this.url.substring(0, this.url.lastIndexOf('/'));
    }
  }

  async createJob() {
    await this.getUrl();
    const body = {
      object: this.options.object,
      operation: this.options.operation
    };
    if (this.options.externalIdFieldName) {
      body.externalIdFieldName = this.options.externalIdFieldName;
    }
    const response = await axios.post(
      `${this.url}/services/data/v${this.options.apiVersion}/jobs/ingest`,
      body, {
        headers: {
          authorization: `Bearer ${this.authToken}`
        }
      });
    this.jobId = response.data.id;
  }

  async getJobInfo(id) {
    id = id || this.jobId;
    await this.getUrl();
    const response = await axios.get(
      `${this.url}/services/data/v${this.options.apiVersion}/jobs/ingest/${id}`, {
        headers: {
          authorization: `Bearer ${this.authToken}`
        }
      });
    return response.data;
  }

  async abortJob(id) {
    return await this.abortComplete(id, 'Aborted');
  }

  async closeJob(id) {
    return await this.abortComplete(id, 'UploadComplete');
  }

  async abortComplete(id, myState) {
    id = id || this.jobId;
    await this.getUrl();
    const body = {
      state: myState
    };
    const response = await axios.patch(
      `${this.url}/services/data/v${this.options.apiVersion}/jobs/ingest/${id}`,
      body, {
        headers: {
          authorization: `Bearer ${this.authToken}`,
          Accept: 'application/json'
        }
      });
    return response.data;
  }

  async deleteJob(id) {
    id = id || this.jobId;
    await this.getUrl();
    await axios.delete(
      `${this.url}/services/data/v${this.options.apiVersion}/jobs/ingest/${id}`, {
        headers: {
          authorization: `Bearer ${this.authToken}`
        }
      });
  }

  async getSuccessfulResults(id) {
    return await this.getResults(id, 'successfulResults');
  }

  async getFailedResults(id) {
    return await this.getResults(id, 'failedResults');
  }

  async getUnprocessedRecords(id) {
    return await this.getResults(id, 'unprocessedrecords');
  }

  async getResults(id, resultType) {
    id = id || this.jobId;
    await this.getUrl();
    return axios.get(
      `${this.url}/services/data/v${this.options.apiVersion}/jobs/ingest/${id}/${resultType}/`, {
        responseType: 'stream',
        headers: {
          authorization: `Bearer ${this.authToken}`
        }
      });
  }
}

exports = module.exports = BulkApi;

// ## OAUTH

// https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_understanding_username_password_oauth_flow.htm

// ### Get Access Token

// ```bash
// curl -X POST \
//   ${BULK2_URL}/services/oauth2/token \
//   -H 'content-type: application/x-www-form-urlencoded' \
//   --data-urlencode "grant_type=password" \
//   --data-urlencode "client_id=${BULK2_CONSUMER_KEY}" \
//   --data-urlencode "client_secret=${BULK2_CONSUMER_SECRET}" \
//   --data-urlencode "username=${BULK2_USERNAME}" \
//   --data-urlencode "password=${BULK2_PASSWORD}${BULK2_TOKEN}"

// {
//   "access_token": "***",
//   "id": "https://login.salesforce.com/id/00D36000000arMLEAY/00536000000IigRAAS",
//   "instance_url": "https://na30.salesforce.com",
//   "issued_at": "1511895199410",
//   "signature": "mbYmf6mq3OcLltEtSgKLNDiJwZhbmB3S7xPf3GGbqp4=",
//   "token_type": "Bearer"
// }
// ```

// ### Get URL for REST Calls

// The following shows how to get the URL for REST calls. Really just need the
// https://na30.salesforce.com bit out of one of the URLs below.

// ```bash
// curl -X GET \
//   https://login.salesforce.com/id/00D36000000arMLEAY/00536000000IigRAAS \
//   -H 'authorization: Bearer ***'

// {
//     "active": true,
//     "addr_city": null,
//     "addr_country": "US",
//     "addr_state": null,
//     "addr_street": null,
//     "addr_zip": null,
//     "asserted_user": true,
//     "display_name": "Pipe Dream",
//     "email": "sfadmin@candoris.com",
//     "email_verified": true,
//     "first_name": "Pipe",
//     "id": "https://login.salesforce.com/id/00D36000000arMLEAY/00536000000IigRAAS",
//     "is_app_installed": true,
//     "is_lightning_login_user": false,
//     "language": "en_US",
//     "last_modified_date": "2016-12-09T04:02:56.000+0000",
//     "last_name": "Dream",
//     "locale": "en_US",
//     "mobile_phone": null,
//     "mobile_phone_verified": false,
//     "nick_name": "pipedream1.457990354178172E12",
//     "organization_id": "00D36000000arMLEAY",
//     "photos": {
//         "picture": "https://c.na30.content.force.com/profilephoto/005/F",
//         "thumbnail": "https://c.na30.content.force.com/profilephoto/005/T"
//     },
//     "status": {
//         "body": null,
//         "created_date": null
//     },
//     "timezone": "America/Los_Angeles",
//     "urls": {
//         "enterprise": "https://na30.salesforce.com/services/Soap/c/{version}/00D36000000arML",
//         "feed_elements": "https://na30.salesforce.com/services/data/v{version}/chatter/feed-elements",
//         "feed_items": "https://na30.salesforce.com/services/data/v{version}/chatter/feed-items",
//         "feeds": "https://na30.salesforce.com/services/data/v{version}/chatter/feeds",
//         "groups": "https://na30.salesforce.com/services/data/v{version}/chatter/groups",
//         "metadata": "https://na30.salesforce.com/services/Soap/m/{version}/00D36000000arML",
//         "partner": "https://na30.salesforce.com/services/Soap/u/{version}/00D36000000arML",
//         "profile": "https://na30.salesforce.com/00536000000IigRAAS",
//         "query": "https://na30.salesforce.com/services/data/v{version}/query/",
//         "recent": "https://na30.salesforce.com/services/data/v{version}/recent/",
//         "rest": "https://na30.salesforce.com/services/data/v{version}/",
//         "search": "https://na30.salesforce.com/services/data/v{version}/search/",
//         "sobjects": "https://na30.salesforce.com/services/data/v{version}/sobjects/",
//         "tooling_rest": "https://na30.salesforce.com/services/data/v{version}/tooling/",
//         "tooling_soap": "https://na30.salesforce.com/services/Soap/T/{version}/00D36000000arML",
//         "users": "https://na30.salesforce.com/services/data/v{version}/chatter/users"
//     },
//     "user_id": "00536000000IigRAAS",
//     "user_type": "STANDARD",
//     "username": "pipedream@candoris.com",
//     "utcOffset": -28800000
// }
// ```