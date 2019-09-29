var _ = require('lodash');
var traverse = require('traverse');

var ARRAY_TYPE_REGEXP = /^\[(\S+)\]$/;

function isType(value, type) {
    var hasType = _.isString(type);
    if (hasType && type.indexOf('|') > -1) {
        return _.any(type.split('|'), function(t) {
            return isType(value, t);
        });
    } else if (hasType && ARRAY_TYPE_REGEXP.test(type)) {
        var t = type.slice(1, -1);
        return _.isArray(value) && _.all(value, function(v) {
            return isType(v, t);
        });
    } else if (hasType && type === '*') {
        return true;
    } else if (hasType && _.isFunction(_['is' + type])) {
        return _['is' + type](value);
    } else {
        return false;
    }
};

function extractParams(obj, schema) {
    return _.reduce(schema, function(extracted, type, key) {
        var value = _.get(obj, key);
        if (!_.isUndefined(value) && isType(value, type)) {
            _.set(extracted, key, value);
        }
        return extracted;
    }, {});
};

function extractAttributes(obj, attrs) {
    return _.reduce(attrs, function(extracted, key) {
        var value = _.get(obj, key);
        if (!_.isUndefined(value)) {
            _.set(extracted, key, value);
        }
        return extracted;
    }, {});
};

function extractObject(obj, mapping, merge) {
    var mapped = {}; // from => to
    var mappedKeys = [];
    _.each(mapping || {}, function(to, from) {
        if (_.isNull(from) || _.isNull(to)) return; // skip
        var value = _.get(obj, from);
        if (!_.isUndefined(value)) {
            mappedKeys.push(from);
            _.set(mapped, to, value);
        }
    });
    if (merge) {
        var unmappedKeys = _.difference(_.keys(obj), mappedKeys);
        return _.extend(extractAttributes(obj, unmappedKeys), mapped);
    }
    return mapped;
};

function formatKeys(obj, callback, formatValue) {
    if (_.isString(callback)) callback = _[callback].bind(_);
    formatValue = _.isFunction(formatValue) ? formatValue : null;
    var cloned = _.cloneDeep(obj);
    traverse(cloned).forEach(function(val) {
        if (this.key && _.isString(this.key)) {
            this.delete();
            this.key = callback(this.key);
            val = formatValue ? formatValue(this.key, val) : val;
            if (_.isObject(val) && val._bsontype) val = String(val);
            this.update(val);
        }
    });
    return cloned;
};

function traverseObject(obj, callback) {
    if (_.isString(callback)) callback = _[callback].bind(_);
    var cloned = _.cloneDeep(obj);
    traverse(cloned).forEach(function(val) {
        callback(this, val);
    });
    return cloned;
};

function normalizeQuery(obj, formatValue) {
    return formatKeys(obj, function(key) {
        return key;
    }, formatValue);
};

function buildSubQuery(key, value) {
    var sub = {};
    sub[key] = value;
    return sub;
};

function buildQuery(obj, parentKey) {
    var search = [];
    if (!_.isObject(obj) && parentKey) {
        return buildQuery(buildSubQuery(parentKey, obj));
    }
    if (!_.isObject(obj)) return search;
    var keys = Object.keys(obj);
    _.each(obj, function(value, key) {
        var q;
        switch (key) {
            case 'and':
            case 'or':
                var logic = key === 'or' ? ' OR ' : ' AND ';
                if (_.isArray(value) && value.length === 1 && parentKey) {
                    search.push(buildQuery(value[0], key));
                } else if (_.isArray(value) && value.length > 0) {
                    var q = _.map(value, buildQuery).join(logic);
                    search.push(parentKey ? '(' + q + ')' : q);
                }
                break;
            case 'gt':
                if (parentKey) search.push(parentKey + ' > ' + value);
                break;
            case 'gte':
                if (parentKey) search.push(parentKey + ' >= ' + value);
                break;
            case 'lt':
                if (parentKey) search.push(parentKey + ' < ' + value);
                break;
            case 'lte':
                if (parentKey) search.push(parentKey + ' <= ' + value);
                break;
            case 'between':
                if (parentKey && _.isArray(value) && _.isNumber(value[0]) && _.isNumber(value[1])) {
                    search.push(parentKey + ':' + value[0] + ' TO ' + value[1]);
                }
                break;
            case 'inq':
            case 'nin':
                var conditions = [].concat(value || []);
                if (parentKey && conditions.length > 0) {
                    var nin = key === 'nin';
                    conditions = _.map(conditions, function(value) {
                        return (nin ? 'NOT ' : '') + parentKey + ':' + quoteArgument(value);
                    });
                    search.push('(' + conditions.join(' OR ') + ')');
                }
                break;
            case 'neq':
                search.push('NOT ' + buildQuery(value, parentKey));
                break
            case 'near':
            case 'like':
            case 'nlike':
                break;
            default:
                if (_.isPlainObject(value)) {
                    search.push(buildQuery(value, key));
                } else {
                    search.push(key + ':' + quoteArgument(value));
                }
        }
    });
    var query = search.join(' AND ');
    return search.length > 1 && parentKey ? '(' + query + ')' : query;
};

function quoteArgument(v) {
    if (_.isString(v)) return '"' + v + '"';
    return v;
};

/*!
 * Merge query parameters
 * @param {Object} base The base object to contain the merged results
 * @param {Object} update The object containing updates to be merged
 * @param {Object} spec Optionally specifies parameters to exclude (set to false)
 * @returns {*|Object} The base object
 * @private
 */
function mergeQuery(base, update, spec) {
    if (!update) return;
    spec = spec || {};
    base = base || {};
    
    if (update.where && Object.keys(update.where).length > 0) {
        if (base.where && Object.keys(base.where).length > 0) {
            base.where = {and: [base.where, update.where]};
        } else {
            base.where = update.where;
        }
    }
    
    // Merge inclusion
    if (spec.include !== false && update.include) {
        if (!base.include) {
            base.include = update.include;
        } else {
            if (spec.nestedInclude === true){
                //specify nestedInclude=true to force nesting of inclusions on scoped
                //queries. e.g. In physician.patients.getAsync({include: 'address'}),
                //inclusion should be on patient model, not on physician model.
                var saved = base.include;
                base.include = {};
                base.include[update.include] = saved;
            } else{
                //default behaviour of inclusion merge - merge inclusions at the same
                //level. - https://github.com/strongloop/loopback-datasource-juggler/pull/569#issuecomment-95310874
                base.include = mergeIncludes(base.include, update.include);
            }
        }
    }
    
    if (spec.collect !== false && update.collect) {
        base.collect = update.collect;
    }
    
    // Overwrite fields
    if (spec.fields !== false && update.fields !== undefined) {
        base.fields = update.fields;
    } else if (update.fields !== undefined) {
        base.fields = [].concat(base.fields).concat(update.fields);
    }
    
    // set order
    if ((!base.order || spec.order === false) && update.order) {
        base.order = update.order;
    }
    
    // overwrite pagination
    if (spec.limit !== false && update.limit !== undefined) {
        base.limit = update.limit;
    }
    
    var skip = spec.skip !== false && spec.offset !== false;
    
    if (skip && update.skip !== undefined) {
        base.skip = update.skip;
    }
    
    if (skip && update.offset !== undefined) {
        base.offset = update.offset;
    }
    
    return base;
};

module.exports = {
    formatKeys: formatKeys,
    extractParams: extractParams,
    extractObject: extractObject,
    traverseObject: traverseObject,
    normalizeQuery: normalizeQuery,
    buildQuery: buildQuery,
    mergeQuery: mergeQuery
};
