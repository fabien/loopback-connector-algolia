var should = require('should');
var registry = require('./init');
var path = require('path');
var _ = require('lodash');

var items = require('./fixtures/contacts.json').slice(0, 20);

if (!process.env.TEST_ALL) return; // disabled by default, to not trigger operations

describe('Indexing', function() {
    
    var Contact;
    var connector;
    var ids = {};
    
    before(function(next) {
        registry.connect(function(err, models) {
            Contact = models.Contact;
            connector = Contact.dataSource.connector;
            Contact.dataSource.automigrate(next);
        });
    });
    
    after(function(next) {
        registry.disconnect(next);
    });
    
    it('should set index settings', function(next) {
        Contact.setIndexSettings({ hitsPerPage: 50 }, {
            wait: true
        }, function(err, settings) {
            if (err) return next(err);
            settings.should.eql({ hitsPerPage: 50 });
            next();
        });
    });
    
    it('should get index settings', function(next) {
        var indexSettings = connector.getModelIndexSettings('Contact');
        var keys = _.keys(indexSettings);
        Contact.getIndexSettings(function(err, settings) {
            if (err) return next(err);
            settings.hitsPerPage.should.equal(50);
            indexSettings.should.eql(_.pick(settings, keys));
            next();
        });
    });
    
    it('should ensure a synonym - add (1)', function(next) {
        var synonym = {
            objectID: 'car',
            synonyms: ['auto', 'automobile']
        };
        Contact.ensureSynonym(synonym, function(err, result) {
            if (err) return next(err);
            result.should.eql(_.extend({ type: 'synonym' }, synonym));
            next();
        });
    });
    
    it('should ensure a synonym - add (2)', function(next) {
        var synonym = {
            objectID: 'tablet',
            type: 'oneWaySynonym',
            input: 'tablet',
            synonyms: ['ipad', 'galaxy note']
        };
        Contact.ensureSynonym(synonym, function(err, result) {
            if (err) return next(err);
            result.should.eql(synonym);
            next();
        });
    });
    
    it('should ensure a synonym - update', function(next) {
        var synonym = {
            objectID: 'car',
            synonyms: ['auto', 'automobile', 'vehicle']
        };
        Contact.ensureSynonym(synonym, function(err, result) {
            if (err) return next(err);
            result.should.eql(_.extend({ type: 'synonym' }, synonym));
            next();
        });
    });
    
    it('should get a synonym', function(next) {
        Contact.getSynonym('car', function(err, synonym) {
            if (err) return next(err);
            synonym.should.eql({
                objectID: 'car',
                type: 'synonym',
                synonyms: ['auto', 'automobile', 'vehicle']
            });
            next();
        });
    });
    
    it('should list synonyms', function(next) {
        var meta = {};
        Contact.listSynonyms({}, { meta: meta }, function(err, synonyms) {
            if (err) return next(err);
            meta.should.eql({ nbHits: 2 });
            var car = _.find(synonyms, { objectID: 'car' });
            _.omit(car, '_highlightResult').should.eql({
                objectID: 'car',
                type: 'synonym',
                synonyms: ['auto', 'automobile', 'vehicle']
            });
            var tablet = _.find(synonyms, { objectID: 'tablet' });
            _.omit(tablet, '_highlightResult').should.eql({
                objectID: 'tablet',
                type: 'oneWaySynonym',
                input: 'tablet',
                synonyms: ['ipad', 'galaxy note']
            });
            next();
        });
    });
    
    it('should delete a synonym', function(next) {
        Contact.deleteSynonym('tablet', function(err, result) {
            if (err) return next(err);
            result.should.have.property('deletedAt');
            next();
        });
    });
    
    it('should list synonyms', function(next) {
        var meta = {};
        Contact.listSynonyms({}, { meta: meta }, function(err, synonyms) {
            if (err) return next(err);
            meta.should.eql({ nbHits: 1 });
            _.pluck(synonyms, 'objectID').should.eql(['car']);
            next();
        });
    });
    
    it('should serialize all given items', function() {
        var indexedProps = _.keys(Contact.definition.properties);
        var expected = _.map(items, function(item) {
            item = _.pick(item, indexedProps);
            item.isAdmin = false;
            return item;
        });
        Contact.serializeForIndex(items).should.eql(expected);
    });
    
    // The workflow below is similar to the suggested zero-downtime
    // reindexing process described by Algolia:
    //
    // 1. create a new - temporary - index with the same settings as the original
    // 2. index all given items
    // 3. adopt the temporary index (uses moveIndex internally)
    
    it('should clone an existing index', function(next) {
        Contact.cloneIndex('tmp_contacts', function(err, index) {
            if (err) return next(err);
            index.should.be.an.object;
            index.search.should.be.a.function;
            next();
        });
    });
    
    it('should index all given items', function(next) {
        Contact.buildIndex(items, {
            indexName: 'tmp_contacts', batchSize: 5
        }, function(err, batches) {
            if (err) return next(err);
            batches.should.be.an.array;
            batches.should.have.length(4);
            _.all(batches, function(b) { return b.length === 5; }).should.be.true;
            next();
        });
    });
    
    it('should adopt another index', function(next) {
        Contact.adoptIndex('tmp_contacts', function(err, index) {
            if (err) return next(err);
            index.should.be.an.object;
            index.search.should.be.a.function;
            next();
        });
    });
    
    it('should have adopted another index', function(next) {
        Contact.count(function(err, count) {
            if (err) return next(err);
            count.should.eql(20);
            next();
        });
    });

});
