# loopback-connector-algolia

Loopback connector for Algolia index management.

Connector config example:

``` js
{
    "applicationId": "<application ID>",
    "apiKey": "<admin API key>",
    "mapping": {                      // from Loopback to Algolia
        "favoriteColor": "favorite_color",
        "group": "demo"
    },
    "attributes": {                   // enforced data attributes
        "group": "demo"
    },
    "defaults": {                     // default data attributes
        "favoriteColor": "red"
    },
    "queryScope": {                   // default/enforced query scope (Loopback)
        "where": { "group": "demo" }
    }
}
```

Note: create a file `test/credentials.local.json` with the following parameters:


``` json
{
    "applicationId": "<application ID>",
    "apiKey": "<admin API key>"
}
```
