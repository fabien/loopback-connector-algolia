var juggler = require('loopback-datasource-juggler');
var Registry = require('independent-juggler');
var registry = new Registry(juggler, { dir: __dirname });

var Connector = require('../..');
var credentials = require('../credentials.local.json');

registry.setupDataSource('algolia', {
    connector: Connector,
    applicationId: credentials.applicationId,
    apiKey: credentials.apiKey,
    mapping: {  // from Loopback to Algolia
        'tags': '_tags'
    },
    attributes: {},  // enforced data attributes
    defaults: {},    // default data attributes
    queryScope: {}   // enforced scope
});

module.exports = registry;
