var assert = require('assert');
var util = require('util');
var _ = require('lodash');
var async = require('async');
var cuid = require('cuid');

var extractParams = require('./utils').extractParams;
var mergeQuery = require('./utils').mergeQuery;
var buildQuery = require('./utils').buildQuery;
var normalizeQuery = require('./utils').normalizeQuery;
var formatKeys = require('./utils').formatKeys;
var extractObject = require('./utils').extractObject;
var traverseObject = require('./utils').traverseObject;

var algoliasearch = require('algoliasearch');

var Connector = require('loopback-connector').Connector;

var COMMON_SETTINGS = {
    'attributesToRetrieve': '[String]',
    'maxValuesPerFacet': 'Number',
    'attributesToHighlight': '[String]',
    'attributesToSnippet': '[String]',
    'highlightPreTag': 'String',
    'highlightPostTag': 'String',
    'snippetEllipsisText': 'String',
    'restrictHighlightAndSnippetArrays': 'Boolean',
    'hitsPerPage': 'Number',
    'minWordSizefor1Typo': 'Number',
    'minWordSizefor2Typos': 'Number',
    'typoTolerance': 'Boolean|String',
    'allowTyposOnNumericTokens': 'Boolean',
    'ignorePlurals': 'Boolean|String',
    'disableTypoToleranceOnAttributes': '[String]',
    'queryType': 'String',
    'removeWordsIfNoResults': 'String',
    'advancedSyntax': 'Boolean',
    'optionalWords': '[String]|String',
    'removeStopWords': '[String]|Boolean',
    'disableExactOnAttributes': '[String]',
    'exactOnSingleWordQuery': 'String',
    'alternativesAsExact': '[String]',
    'distinct': 'Number|Boolean',
    'replaceSynonymsInHighlight': 'Boolean',
    'minProximity': 'Number',
    'responseFields': '[String]',
    'maxFacetHits': 'Number'
};

var VALID_QUERY_SETTINGS = _.extend({
    'restrictSearchableAttributes': '[String]',
    'filters': 'String',
    'facets': '[String]',
    'facetingAfterDistinct': 'Boolean',
    'facetFilters': 'Array',
    'page': 'Number',
    'offset': 'Number',
    'length': 'Number',
    'aroundLatLng': 'String',
    'aroundLatLngViaIP': 'Boolean',
    'aroundRadius': 'Number|String',
    'aroundPrecision': 'Number',
    'minimumAroundRadius': 'Number',
    'insideBoundingBox': '*',
    'insidePolygon': '*',
    'getRankingInfo': 'Boolean',
    'numericFilters': '[String]',
    'tagFilters': '[String]',
    'analytics': 'Boolean',
    'analyticsTags': '[String]',
    'synonyms': 'Boolean'
}, COMMON_SETTINGS);

var VALID_SETTINGS = _.extend({
    'searchableAttributes': '[String]',
    'attributesForFaceting': '[String]',
    'unretrievableAttributes': '[String]',
    'ranking': '[String]',
    'customRanking': '[String]',
    'replicas': '[String]',
    'paginationLimitedTo': 'Number',
    'disableTypoToleranceOnWords': '[String]',
    'separatorsToIndex': 'String',
    'disablePrefixOnAttributes': '[String]',
    'numericAttributesForFiltering': '[String]',
    'allowCompressionOfIntegerArray': 'Boolean',
    'attributeForDistinct': 'String',
    'placeholders': 'Object'
}, COMMON_SETTINGS);

module.exports = Algolia;

function Algolia(settings) {
    assert(typeof settings === 'object', 'cannot initialize Algolia connector without a settings object');
    assert(typeof settings.applicationId === 'string', 'cannot initialize Algolia connector without an application ID');
    assert(typeof settings.apiKey === 'string', 'cannot initialize Algolia connector without an API key');
    
    Connector.call(this, 'algolia', settings);
    
    this.serializeMapping = _.extend({}, settings.serializeMapping || settings.mapping); // from Loopback to Algolia
    this.normalizeMapping = _.extend({}, settings.normalizeMapping || _.invert(this.serializeMapping)); // from Algolia to Loopback
    
    this.serializeMapping['id'] = 'objectID';
    this.normalizeMapping['objectID'] = 'id';
};

util.inherits(Algolia, Connector);

Algolia.initialize = function(dataSource, callback) {
    var connector = new Algolia(dataSource.settings);
    var settings = connector.settings || {};
    var options = _.omit(settings, 'applicationId', 'apiKey');
    dataSource.connector = connector; // Attach connector to dataSource
    connector.dataSource = dataSource; // Hold a reference to dataSource
    connector.client = algoliasearch(settings.applicationId, settings.apiKey, options);
    process.nextTick(callback);
};

Algolia.prototype.define = function(modelDefinition) {
    modelDefinition.properties = modelDefinition.properties || {};
    modelDefinition.properties['id'] = { type: String, id: true };
    Connector.prototype.define.call(this, modelDefinition);
    var connector = this;
    
    modelDefinition.model.algoliaClient = this.client;
    
    modelDefinition.model.filterIndexSettings = function(settings, options) {
        var modelSettings = connector.getModelSettings(this.modelName);
        var validSettings = VALID_SETTINGS;
        if (_.isObject(options) && _.isArray(options.validSettings)) {
            validSettings = _.pick(validSettings, options.validSettings);
        }
        if (_.isArray(modelSettings.validSettings)) {
            validSettings = _.pick(validSettings, modelSettings.validSettings);
        }
        return extractParams(settings, validSettings);
    };
    
    modelDefinition.model.getIndex = function(options) {
        return connector.getIndex(this.modelName, options);
    };
    
    modelDefinition.model.isValidIndex = function(indexName) {
        return connector.isValidIndex(this.modelName, indexName);
    };
    
    modelDefinition.model.buildIndex = function(items, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        
        if (!_.isArray(items)) return callback(new Error('Invalid input'));
        
        var batchSize = options.batchSize || modelDefinition.settings.batchSize;
        batchSize = batchSize || connector.settings.batchSize || 500;
        
        var modelName = this.modelName;
        var index = this.getIndex(options);
        var waitTask = !!options.wait; // default: false
        var serializeData = connector.serializeData.bind(connector, modelName);
        
        var chunks = batchSize > 0 ? _.chunk(items, batchSize) : [items];
        
        async.mapSeries(chunks, function(chunk, next) {
            var serializedItems = _.map(chunk, function(item) {
                return serializeData(item, options);
            });
            var p = index.saveObjects(serializedItems, { autoGenerateObjectIDIfNotExist: true });
            (waitTask ? p.wait() : p).then(function(result) {
                next(null, serializedItems, result);
            }).catch(catchCallback(function(err) {
                next(err, []);
            }));
        }, callback);
    };
    
    modelDefinition.model.cloneIndex = function(newIndexName, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        var index = this.getIndex(options);
        var newIndex = connector.client.initIndex(newIndexName);
        var waitTask = options.wait !== false; // default: true
        var filterIndexSettings = this.filterIndexSettings.bind(this);
        
        if (index.indexName === newIndex.indexName) {
            return callback(new Error('Cannot clone to identical index'));
        }
        
        index.getSettings().then(function(settings) {
            settings = filterIndexSettings(_.omit(settings, 'replicas'), options);
            var setOpts = _.pick(options, 'forwardToReplicas');
            var p = newIndex.setSettings(settings, setOpts);
            return waitTask ? p.wait() : p;
        }).then(function() {
            callback(null, newIndex);
        }).catch(catchCallback(callback));
    };
    
    modelDefinition.model.adoptIndex = function(newIndexName, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        var indexName = connector.getIndexName(this.modelName, options);
        var newIndex = connector.client.initIndex(newIndexName);
        var waitTask = options.wait !== false; // default: true
        var p = connector.client.moveIndex(newIndexName, indexName);
        (waitTask ? p.wait() : p).then(function(result) {
            callback(null, newIndex);
        }).catch(catchCallback(callback));
    };
    
    modelDefinition.model.rebuildIndex = function(items, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        var self = this;
        this.clearIndex(options, function(err) {
            if (err) return callback(err);
            self.buildIndex(items, options, callback);
        });
    };
    
    modelDefinition.model.clearIndex = function(options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        var index = this.getIndex(options);
        index.clearObjects().wait().then(wrapCallback(callback)).catch(catchCallback(callback));
    };
    
    modelDefinition.model.serializeForIndex = function(item, options) {
        var modelName = this.modelName;
        var ctor = this;
        if (_.isArray(item)) {
            return _.map(item, function(obj) {
                var instance = new ctor(obj);
                return connector.serializeData(modelName, instance.toObject(), options);
            });
        } else if (_.isObject(item)) {
            var instance = new ctor(item);
            return connector.serializeData(modelName, instance.toObject(), options);
        }
    };
    
    modelDefinition.model.getIndexSettings = function(options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        var filterIndexSettings = this.filterIndexSettings.bind(this);
        var index = this.getIndex(options);
        index.getSettings().then(function(settings) {
            callback(null, filterIndexSettings(settings, options));
        }).catch(catchCallback(callback));
    };
    
    modelDefinition.model.setIndexSettings = function(settings, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        settings = this.filterIndexSettings(settings, options);
        var waitTask = !!options.wait; // default: false
        var opts = _.pick(options, 'forwardToReplicas');
        var index = this.getIndex(options);
        var p = index.setSettings(settings, opts);
        (waitTask ? p.wait() : p).then(function(result) {
            callback(null, settings, result);
        }).catch(catchCallback(callback));
    };
    
    modelDefinition.model.ensureSynonym = function(data, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        data = _.extend({}, data);
        options = _.extend({}, options);
        if (_.isEmpty(data.objectID)) data.objectID = cuid();
        if (_.isEmpty(data.type)) data.type = 'synonym'; // alt: oneWaySynonym
        var waitTask = options.wait !== false; // default: true
        var opts = _.pick(options, 'forwardToReplicas');
        var index = this.getIndex(options);
        var p = index.saveSynonym(data, opts);
        (waitTask ? p.wait() : p).then(function(result) {
            callback(null, data, result);
        }).catch(catchCallback(callback));
    };
    
    modelDefinition.model.deleteSynonym = function(objectID, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        var waitTask = options.wait !== false; // default: true
        var opts = _.pick(options, 'forwardToReplicas');
        var index = this.getIndex(options);
        var p = index.deleteSynonym(objectID, opts);
        (waitTask ? p.wait() : p).then(wrapCallback(callback)).catch(callback);
    };
    
    modelDefinition.model.getSynonym = function(objectID, options, callback) {
        if (_.isFunction(options)) callback = options, options = {};
        options = _.extend({}, options);
        var index = this.getIndex(options);
        index.getSynonym(objectID).then(wrapCallback(callback)).catch(catchCallback(callback));
    };
    
    modelDefinition.model.listSynonyms = function(searchParams, options, callback) {
        if (_.isFunction(searchParams)) {
            callback = searchParams, searchParams = {}, options = {};
        } else if (_.isFunction(options)) {
            callback = options, options = {};
        }
        searchParams = _.extend({ query: '' }, searchParams);
        var index = this.getIndex(_.extend({}, options));
        index.searchSynonyms(searchParams.query, _.omit(searchParams, 'query')).then(function(response) {
            if (_.isObject(options.meta)) {
                _.extend(options.meta, _.omit(response, 'hits'));
            }
            callback(null, response.hits);
        }).catch(catchCallback(function(err) {
            callback(err, []);
        }));
    };
};

Algolia.prototype.getModelSettings = function(model) {
    var modelClass = this._models[model];
    if (modelClass && modelClass.settings && _.isObject(modelClass.settings.algolia)) {
        return _.extend({}, modelClass.settings.algolia);
    }
    return {};
};

Algolia.prototype.getModelIndexSettings = function(model) {
    var defaults = _.extend({}, this.settings.index);
    var settings = this.getModelSettings(model);
    settings = _.merge({}, defaults, settings);
    settings = _.omit(settings, 'indexName');
    var properties = _.extend({}, this._models[model] && this._models[model].properties);
    
    if (_.isObject(settings.algoliaProperties)) {
        _.extend(properties, settings.algoliaProperties);
    }
    
    if (!_.isArray(settings.searchableAttributes)) {
        var searchable = _.reduce(properties, function(attrs, prop, name) {
            if (_.isNumber(prop.searchable)) {
                attrs.push({ name: name, seq: prop.searchable });
            } else if (prop.searchable === true) {
                attrs.push({ name: name, seq: Infinity });
            }
            return attrs;
        }, []);
        settings.searchableAttributes = _.pluck(_.sortBy(searchable, 'seq'), 'name');
    }
    
    if (!_.isArray(settings.attributesForFaceting)) {
        settings.attributesForFaceting = _.reduce(properties, function(attrs, prop, name) {
            if (prop.facet === true) {
                attrs.push(name);
            } else if (prop.facet === 'filter') {
                attrs.push('filterOnly(' + name + ')');
            } else if (prop.facet === 'searchable') {
                attrs.push('searchable(' + name + ')');
            }
            return attrs;
        }, []);
    }
    
    if (!_.isArray(settings.attributesToRetrieve)) {
        var attributesToRetrieve = _.reduce(properties, function(attrs, prop, name) {
            if (prop.retrievable === true) {
                attrs.push(name);
            }
            return attrs;
        }, []);
        if (!_.isEmpty(attributesToRetrieve)) settings.attributesToRetrieve = attributesToRetrieve;
    }
    
    if (!_.isArray(settings.unretrievableAttributes)) {
        unretrievableAttributes = _.reduce(properties, function(attrs, prop, name) {
            if (prop.unretrievable === true) {
                attrs.push(name);
            }
            return attrs;
        }, []);
        if (!_.isEmpty(unretrievableAttributes)) settings.unretrievableAttributes = unretrievableAttributes;
    }
    
    return settings;
};

Algolia.prototype.isValidIndex = function(model, indexName, modelSettings) {
    modelSettings = modelSettings || this.getModelSettings(model);
    
    var validIndexes = [].concat(modelSettings.validIndexes || []);
    
    if (modelSettings.indexName) {
        validIndexes.push(modelSettings.indexName);
    } else if (this.settings.indexName) {
        validIndexes.push(this.settings.indexName);
    } else {
        validIndexes.push(_.snakeCase(model));
    }
    
    if (_.isEmpty(modelSettings.validIndexes)) {
        validIndexes = validIndexes.concat(_.keys(modelSettings.indexes || {}));
        validIndexes = validIndexes.concat(this.settings.validIndexes || []);
    }
    
    return _.isString(indexName) && _.include(validIndexes, indexName);
};

Algolia.prototype.getIndex = function(model, options) {
    options = options || {};
    var modelSettings = this.getModelSettings(model);
    var indexName = options.indexName || modelSettings.indexName || this.settings.indexName;
    indexName = indexName || _.snakeCase(model);
    
    if (this.isValidIndex(model, indexName, modelSettings)) {
        return this.client.initIndex(indexName);
    } else {
        throw new Error('Invalid index');
    }
};

Algolia.prototype.getIndexName = function(model, options) {
    var indexName;
    if (_.isObject(model) && _.isString(model.indexName)) {
        indexName = model.indexName;
    } else if (_.isString(model)) {
        var index = this.getIndex(model, options);
        indexName = index && index.indexName;
    }
    return indexName;
};

// CRUD

Algolia.prototype.create = function (model, data, options, callback) {
    createInstance.call(this, model, data, options, callback);
};

function isValidObjectID(objectID) {
    if ((_.isString(objectID) && !_.isEmpty(objectID)) || _.isNumber(objectID)) {
        return true;
    } else {
        return false;
    }
};

function createInstance(model, data, options, callback) {
    if (_.isFunction(options)) callback = options, options = {};
    options = _.extend({}, options);
    var self = this;
    
    var modelSettings = this.getModelSettings(model);
    var waitTask = options.wait !== false; // default: true
    var index = this.getIndex(model, options);
    
    data = this.serializeData(model, data || {}, options);
    if (_.isEmpty(data)) {
        callback(new Error('No data'));
    } else {
        if (!isValidObjectID(data.objectID)) data = _.omit(data, 'objectID');
        var p = index.saveObject(data, { autoGenerateObjectIDIfNotExist: true });
        (waitTask ? p.wait() : p).then(function(result) {
            callback(null, result.objectID);
        }).catch(catchCallback(callback));
    }
};

function updateAll(model, where, data, options, callback) {
    var self = this;
    this.all(model, { where: where, attributesToRetrieve: [] }, options, function(err, items) {
        if (err) return callback(err, { count: 0 });
        async.reduce(items, 0, function(memo, instance, next) {
            self.updateAttributes(model, instance.id, data, options, function(err) {
                next(err, err ? memo : memo + 1);
            });
        }, function(err, count) {
            callback(err, { count: count });
        });
    });
};

Algolia.prototype.update = Algolia.prototype.updateAll = updateAll;

Algolia.prototype.save = function save(model, data, options, callback) {
    var id = this.getIdValue(model, data);
    if (_.isEmpty(id)) {
        createInstance.call(this, model, data, options, callback);
    } else {
        options = _.extend({}, options, { saveObject: true });
        this.updateAttributes(model, id, data, options, callback);
    }
};

Algolia.prototype.updateAttributes = function updateAttributes(model, id, data, options, callback) {
    options = _.extend({ serializeDefaults: false }, options);
    var index = this.getIndex(model, options);
    var normalizeData = this.normalizeData.bind(this, model);
    var waitTask = options.wait !== false; // default: true
    var self = this;
    var p;
    
    data = this.serializeData(model, data, options);
    data.objectID = String(id);
    
    if (options.saveObject) {
        p = index.saveObject(data);
    } else {
        p = index.partialUpdateObject(data);
    }
    
    (waitTask ? p.wait() : p).then(function() {
        callback(null, normalizeData(data), { isNewInstance: false });
    }).catch(catchCallback(callback));
};

Algolia.prototype.findMultiple = function fetch(model, ids, options, callback) {
    options = _.extend({}, options);
    var index = this.getIndex(model, options);
    var normalizeData = this.normalizeData.bind(this, model);
    index.getObjects(ids, _.pick(options, 'attributesToRetrieve')).then(function(response) {
        var items = _.compact(response.results);
        callback(null, _.map(items, normalizeData));
    }).catch(catchCallback(function(err) {
        callback(err, []);
    }));
};

Algolia.prototype.find = function find(model, id, options, callback) {
    this.findMultiple(model, [].concat(id || []), options, function(err, items) {
        callback(err, _.isArray(items) ? _.first(items) : null);
    });
};

Algolia.prototype.all = function all(model, filter, options, callback) {
    options = _.extend({}, options);
    var self = this;
    
    if (_.isObject(filter.where) && _.has(filter.where, 'id')) {
        var ids = [];
        if (_.isObject(filter.where.id) && _.isArray(filter.where.id.inq)) {
            ids = ids.concat(filter.where.id.inq);
        } else if (_.isString(filter.where.id) || _.isArray(filter.where.id)) {
            ids = ids.concat(filter.where.id || []);
        }
        self.findMultiple(model, ids, options, callback);
    } else {
        var index = this.getIndex(model, options);
        var normalizeData = this.normalizeData.bind(this, model);
        
        var q = this.buildQuery(model, filter);
        var p = index.search(q.query || '', _.omit(q, 'query'));
        
        p.then(function(response) {
            if (_.isObject(options.meta)) {
                _.extend(options.meta, _.omit(response, 'hits'));
            }
            callback(null, _.map(response.hits, normalizeData));
        }).catch(catchCallback(function(err) {
            callback(err, []);
        }));
    }
};

Algolia.prototype.exists = function exists(model, id, options, callback) {
    var index = this.getIndex(model, options);
    index.getObject(id, { attributesToRetrieve: [] }).then(function(response) {
        callback(null, _.isObject(response) && _.isString(response.objectID));
    }).catch(catchCallback(function(err) {
        callback(err, false);
    }));
};

Algolia.prototype.count = function count(model, where, options, callback) {
    var index = this.getIndex(model, options);
    var q = this.buildQuery(model, { where: where || {}, limit: 1 });
    q.attributesToRetrieve = [];
    q.attributesToHighlight = [];
    q.attributesToSnippet = [];
    
    index.search(q.query || '', _.omit(q, 'query')).then(function(response) {
        callback(null, response ? response.nbHits : 0);
    }).catch(catchCallback(function(err) {
        callback(err, 0);
    }));
};

Algolia.prototype.destroy = function destroy(model, id, options, callback) {
    options = _.extend({}, options);
    var waitTask = options.wait !== false; // default: true
    var index = this.getIndex(model, options);
    var p = index.deleteObject(id);
    (waitTask ? p.wait() : p).then(function() {
        callback(null, { count: 1 });
    }).catch(catchCallback(function(err) {
        callback(err, { count: 0 });
    }));
};

Algolia.prototype.destroyAll = function destroyAll(model, where, options, callback) {
    options = _.extend({}, options);
    var waitTask = options.wait !== false; // default: true
    var index = this.getIndex(model, options);
    
    if (_.isObject(where) && _.has(where, 'id')) {
        var ids = [];
        if (_.isObject(where.id) && _.isArray(where.id.inq)) {
            ids = ids.concat(where.id.inq);
        } else if (_.isString(where.id) || _.isArray(where.id)) {
            ids = ids.concat(where.id || []);
        }
        handlePromise(index.deleteObjects(ids), ids.length);
    } else {
        var q = this.buildQuery(model, { where: where || {} });
        var params = _.omit(q, 'query');
        if (_.isEmpty(q.query) && _.isEmpty(params) && !options.force) {
            return callback(new Error('Cannot destroy without a valid query'), { count: 0 });
        } else {
            this.count(model, where, options, function(err, count) {
                if (err) return callback(err, { count: 0 });
                handlePromise(index.deleteBy(params), count);
            });
        }
    }
    
    function handlePromise(p, deleted) {
        (waitTask ? p.wait() : p).then(function() {
            callback(null, { count: deleted });
        }).catch(catchCallback(function(err) {
            callback(err, { count: 0 });
        }));
    };
};

// Migration

/**
 * Perform autoupdate for the given models. It basically calls ensureIndex
 * @param {String[]} [models] A model name or an array of model names. If not
 * present, apply to all models
 * @param {Function} [cb] The callback function
 */
Algolia.prototype.autoupdate = function(models, cb) {
    if (_.isFunction(models)) cb = models, models = null;
    if (_.isString(models)) {
        models = [models];
    }
    models = models || Object.keys(this._models);
    var self = this;

    async.eachSeries(models, function(model, next) {
        var indexes = _.extend({}, self._models[model].settings.indexes);
        var primaryIndex = self.getIndexName(model);
        indexes[primaryIndex] = self.getModelIndexSettings(model);
        self.migrateIndexes(false, model, indexes, next);
    }, cb);
};

/**
 * Perform automigrate for the given models. It drops the corresponding indexes
 * and calls createIndex
 * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
 * @param {Function} [cb] The callback function
 */
Algolia.prototype.automigrate = function(models, cb) {
    if (_.isFunction(models)) cb = models, models = null;
    if (_.isString(models)) {
        models = [models];
    }
    models = models || Object.keys(this._models);
    var self = this;
    
    async.eachSeries(models, function(model, next) {
        var indexes = _.extend({}, self._models[model].settings.indexes);
        var primaryIndex = self.getIndexName(model);
        indexes[primaryIndex] = self.getModelIndexSettings(model);
        self.migrateIndexes(true, model, indexes, next);
    }, cb);
};

Algolia.prototype.migrateIndexes = function(deleteIndex, model, indexes, cb) {
    var self = this;
    async.eachSeries(_.keys(indexes), function(indexName, next) {
        var indexSettings = indexes[indexName];
        if (deleteIndex) {
            self.deleteIndex(indexName, function(err) {
                if (err) return next(err);
                self.ensureIndex(indexName, indexSettings, next);
            });
        } else {
            self.ensureIndex(indexName, indexSettings, next);
        }
    }, cb);
};

// Index management

Algolia.prototype.listIndices = function(cb) {
    this.client.listIndices().then(function(result) {
        if (_.isArray(result.items)) {
            cb(null, result.items);
        } else {
            cb(null, []);
        }
    }).catch(catchCallback(cb));
};

Algolia.prototype.indexExists = function(indexName, cb) {
    this.listIndices(function(err, indexes) {
        if (err) return cb(err);
        var indexNames = _.pluck(indexes, 'name');
        return cb(null, _.include(indexNames, indexName));
    });
};

// Create or update an index

Algolia.prototype.ensureIndexForModel = function(model, settings, cb) {
    this.ensureIndex(this.getIndexName(model), settings, cb);
};

Algolia.prototype.ensureIndex = function(indexName, settings, cb) {
    var index = this.client.initIndex(indexName);
    index.setSettings(settings).wait().then(wrapCallback(cb)).catch(catchCallback(cb));
};

// Update index, only if it exists

Algolia.prototype.updateIndexForModel = function(model, settings, cb) {
    this.updateIndex(this.getIndexName(model), settings, cb);
};

Algolia.prototype.updateIndex = function(indexName, settings, cb) {
    if (_.isFunction(settings)) cb = settings, settings = {};
    var self = this;
    this.indexExists(indexName, function(err, exists) {
        if (err) return cb(err);
        self.ensureIndex(indexName, settings, cb);
    });
};

// Delete index

Algolia.prototype.deleteIndexForModel = function(model, options, cb) {
    this.deleteIndex(this.getIndexName(model), options, cb);
};

Algolia.prototype.deleteIndex = function(indexName, options, cb) {
    if (_.isFunction(options)) cb = options, options = {};
    options = options || {};
    var self = this;
    
    this.indexExists(indexName, function(err, exists) {
        if (err) return cb(err);
        var index = exists ? self.client.initIndex(indexName) : null;
        if (options.strict && !exists) { // strict: throw if not exists
            cb(new Error('Invalid index: ' + indexName));
        } else if (index && options.clear === true) {
            index.clearObjects().wait().then(wrapCallback(cb)).catch(catchCallback(cb));
        } else if (index) {
            index.delete().wait().then(wrapCallback(cb)).catch(catchCallback(cb));
        } else {
            cb(null);
        }
    });
};

// Data handling

Algolia.prototype.normalizeFields = function(model, fields) {
    return _.map(fields, this.normalizeField.bind(this, model));
};

Algolia.prototype.normalizeField = function(model, field) {
    var mapping = this.getSerializeMapping(model);
    return mapping[field] || field;
};

Algolia.prototype.buildWhere = function(model, where) {
    var where = _.extend({}, where);
    var normalizeField = this.normalizeField.bind(this, model);
    var query = '';
    
    if (!_.isEmpty(where)) {
        where = normalizeQuery(where, function(key, val) { // first
            return normalizeField(val);
        });
        where = formatKeys(where, normalizeField);
        query = buildQuery(where);
    }
    
    return query;
};

Algolia.prototype.buildQuery = function(model, filter) {
    var modelSettings = this.getModelSettings(model);
    filter = _.extend({}, filter);
    var query = {};
    
    if (_.isString(filter.query)) {
        query.query = filter.query;
    }
    
    if (_.isObject(filter.where)) {
        var where = _.extend({}, filter.where);
        where = mergeQuery(where, this.settings.queryScope || {});
        where = mergeQuery(where, modelSettings.queryScope || {});
        var filters = this.buildWhere(model, where);
        if (!_.isEmpty(filters)) query.filters = filters;
    } else if (_.isString(filter.filters) && !_.isEmpty(filter.filters)) {
        query.filters = filter.filters;
    }
    
    if (_.isNumber(filter.page)) {
        query.page = filter.page;
        if (_.isNumber(filter.limit)) {
            query.hitsPerPage = filter.limit;
        }
        delete filter.limit;
    } else if (_.isNumber(filter.limit)) {
        query.hitsPerPage = filter.limit;
        var offset = 0;
        if (_.isNumber(filter.offset) && filter.offset > 0) {
            offset = filter.offset;
        } else if (_.isNumber(filter.skip) && filter.skip > 0) {
            offset = filter.skip;
        }
        delete filter.offset;
        delete filter.skip;
        var page = Math.floor(offset / filter.limit);
        if (page > 0) query.page = page;
    }
    
    if (_.isArray(filter.fields) && !_.isEmpty(filter.fields)) {
        query.attributesToRetrieve = this.normalizeFields(model, filter.fields);
    }
    
    _.pull(query.attributesToRetrieve, 'objectID');
    
    _.extend(query, extractParams(filter, VALID_QUERY_SETTINGS));
    
    if (filter.facets === '*') query.facets = '*';
    if (filter.attributesToHighlight === '*') query.attributesToHighlight = '*';
    if (filter.attributesToSnippet === '*') query.attributesToSnippet = '*';
    
    where = mergeQuery(where, _.omit(this.settings.queryScope || {}, 'where'));
    where = mergeQuery(where, _.omit(modelSettings.queryScope || {}, 'where'));
    
    return query;
};

Algolia.prototype.getNormalizeMapping = function(model) {
    var modelSettings = this.getModelSettings(model);
    var customMapping = modelSettings.normalizeMapping || _.invert(this.getSerializeMapping(model));
    return _.extend({}, this.normalizeMapping, customMapping);
};

Algolia.prototype.getSerializeMapping = function(model) {
    var modelSettings = this.getModelSettings(model);
    var customMapping = modelSettings.serializeMapping || modelSettings.mapping;
    return _.extend({}, this.serializeMapping, customMapping);
};

Algolia.prototype.normalizeData = function(model, data) {
    var mapping = this.getNormalizeMapping(model);
    var data = extractObject(data, mapping, true);
    return this.coerceData(model, data, _.invert(mapping), true);
};

Algolia.prototype.serializeData = function(model, data, options) {
    var modelSettings = this.getModelSettings(model);
    var mapping = this.getSerializeMapping(model);
    data = _.extend({}, data);
    options = _.extend({}, options);
    
    data = this.coerceData(model, data, mapping);
    
    if (options.serializeDefaults !== false) {
        setDefaults(data, this.settings.defaults, options);
        setDefaults(data, modelSettings.defaults, options);
    }
    
    setAttributes(data, this.settings.attributes, options);
    setAttributes(data, modelSettings.attributes, options);
    
    data = extractObject(data, mapping, true);
    
    if (_.isArray(options.omit)) data = _.omit(data, options.omit);
    if (_.isArray(options.pick)) data = _.pick(data, options.pick);
    
    return data;
};

Algolia.prototype.coerceData = function(model, data, mapping, normalize) {
    var definition = this._models[model];
    if (definition) {
        var coerceTimestamp = this.coerceTimestamp.bind(this, model);
        _.each(definition.properties, function(prop, name) {
            if (prop.type !== Date || _.isUndefined(data[name])) return;
            var value = coerceTimestamp(data[name], normalize);
            if (!_.isUndefined(value)) data[name] = value;
        });
        
        if (definition.settings && definition.settings.strict) {
            var validKeys = _.keys(definition.properties);
            validKeys = _.union(validKeys, _.keys(mapping));
            data = _.pick(data, validKeys);
        }
    }
    return data;
};

Algolia.prototype.coerceTimestamp = function(model, ts, normalize) {
    var modelSettings = this.getModelSettings(model);
    if (_.isFunction(modelSettings.coerceTimestamp)) {
        return modelSettings.coerceTimestamp(ts, normalize);
    } else {
        return normalize ? fromTimestamp(ts) : toTimestamp(ts);
    }
};

function wrapCallback(cb) {
    return function(result) {
        if (_.isFunction(cb)) cb(null, result);
    };
};

function catchCallback(cb) {
    return function(err) {
        if (!(err instanceof Error)) {
            err = new Error(_.isObject(err) ? err.name : 'Algolia Error');
        }
        if (_.isFunction(cb)) cb(err);
    };
};

function toTimestamp(ts) {
    if (_.isNumber(ts)) return ts;
    if (_.isDate(ts)) return Math.floor(ts.getTime() / 1000);
};

function fromTimestamp(ts) {
    if (_.isDate(ts)) return ts;
    if (_.isNumber(ts)) return new Date(ts * 1000);
};

function setDefaults(data, source, options) {
    if (_.isObject(source)) {
        _.defaults(data, source);
    } else if (_.isFunction(source)) {
        _.defaults(data, source(model, data, options));
    }
};

function setAttributes(data, source, options) {
    if (_.isObject(source)) {
        _.merge(data, source);
    } else if (_.isFunction(source)) {
        _.merge(data, source(model, data, options));
    }
};
