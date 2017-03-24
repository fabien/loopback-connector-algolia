var should = require('should');
var registry = require('./init');
var path = require('path');
var _ = require('lodash');

var items = require('./fixtures/contacts.json').slice(0, 20);
var indexedProps = ['state', 'city', 'company', 'firstname', 'lastname', 'followers'];

return; // disabled by default, to not trigger operations

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
    
    before(function(next) {
        setTimeout(next, 1000); // wait on automigrate
    });
    
    after(function(next) {
        registry.disconnect(next);
    });
    
    it('should serialize all given items', function() {
        var expected = _.map(items, function(item) {
            return _.pick(item, indexedProps);
        });
        Contact.serializeForIndex(items).should.eql(expected);
    });
    
    it('should not have any indexed items after automigrate', function(next) {
        Contact.count(function(err, count) {
            if (err) return next(err);
            count.should.eql(0);
            next();
        });
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
