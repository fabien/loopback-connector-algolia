var should = require('should');
var registry = require('./init');
var path = require('path');
var _ = require('lodash');
var mergeQuery = require('../lib/utils').mergeQuery;

// Note: fixtures/contacts.json should be imported into
// an index called 'dev_contacts' with these settings:
//
// var index = Contact.dataSource.client.initIndex('dev_contacts');
//
// var index.setSettings({
//     searchableAttributes: ['firstname', 'lastname', 'city', 'county', 'followers'],
//     attributesForFaceting: ['city', 'state', 'followers']
// });
//
// index.addObjects(require('fixtures/contacts.json'), function(err, result) {
//     console.log(err ? err : result);
// });

describe('Connector', function() {
    
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
    
    var contactData = {
        firstname: 'Fabien', lastname: 'Franzen',
        company: 'Atelier Fabien',
        city: 'Portland', state: 'OR',
        followers: 1327,
        registeredAt: new Date('2017-03-24'),
        tags: ['foo', 'bar', 'baz']
    };
    
    it('should attached DAO methods and properties', function() {
        Contact.algoliaClient.should.equal(connector.client);
        Contact.serializeForIndex.should.be.a.function;
        Contact.rebuildIndex.should.be.a.function;
        Contact.buildIndex.should.be.a.function;
        Contact.clearIndex.should.be.a.function;
        Contact.adoptIndex.should.be.a.function;
    });
    
    it('should create a query', function() {
        connector.buildQuery('Contact').should.eql({});
        connector.buildQuery('Contact', {
            fields: ['firstname', 'lastname']
        }).should.eql({
            attributesToRetrieve: ['firstname', 'lastname']
        });
        connector.buildQuery('Contact', {
            query: 'state:OR AND followers > 1000'
        }).should.eql({ query: 'state:OR AND followers > 1000' });
        connector.buildQuery('Contact', { fields: ['firstname', 'lastname'] }).should.eql({
            attributesToRetrieve: ['firstname', 'lastname']
        });
        connector.buildQuery('Contact', {
            query: 'state:OR AND followers > 1000',
            restrictSearchableAttributes: ['firstname'],
            typoTolerance: 'min',
            facets: ['state'],
            attributesToHighlight: 'invalid',
            unknown: 'invalid'
        }).should.eql({
            query: 'state:OR AND followers > 1000',
            restrictSearchableAttributes: ['firstname'],
            typoTolerance: 'min',
            facets: ['state']
        });
        connector.buildQuery('Contact', { where: { state: { inq: ['OR', 'CA'] } } }).should.eql({
            filters: '(state:"OR" OR state:"CA")'
        });
        connector.buildQuery('Contact', { filters: 'state:"OR" OR state:"CA"' }).should.eql({
            filters: 'state:"OR" OR state:"CA"'
        });
    });
    
    it('should paginate a query', function() {
        connector.buildQuery('Contact', { limit: 5 }).should.eql({
            hitsPerPage: 5
        });
        connector.buildQuery('Contact', { offset: 4, limit: 5 }).should.eql({
            hitsPerPage: 5
        });
        connector.buildQuery('Contact', { offset: 5, limit: 5 }).should.eql({
            page: 1, hitsPerPage: 5
        });
        connector.buildQuery('Contact', { offset: 7, limit: 5 }).should.eql({
            page: 1, hitsPerPage: 5
        });
        connector.buildQuery('Contact', { offset: 10, limit: 5 }).should.eql({
            page: 2, hitsPerPage: 5
        });
        connector.buildQuery('Contact', { offset: 14, limit: 5 }).should.eql({
            page: 2, hitsPerPage: 5
        });
        connector.buildQuery('Contact', { offset: 15, limit: 5 }).should.eql({
            page: 3, hitsPerPage: 5
        });
        connector.buildQuery('Contact', { page: 2, limit: 5 }).should.eql({
            page: 2, hitsPerPage: 5
        });
    });
    
    it('should create a search query', function() {
        connector.buildWhere('Contact').should.eql('');
        connector.buildWhere('Contact', {
            state: 'OR', city: 'Salem'
        }).should.eql('state:"OR" AND city:"Salem"');
        connector.buildWhere('Contact', {
            state: { inq: ['OR', 'CA'] }
        }).should.eql('(state:"OR" OR state:"CA")');
        connector.buildWhere('Contact', {
            state: { nin: ['OR', 'CA'] }
        }).should.eql('(NOT state:"OR" OR NOT state:"CA")');
        connector.buildWhere('Contact', {
            state: { neq: 'CA' }
        }).should.eql('NOT state:"CA"');
        connector.buildWhere('Contact', {
            followers: { gt: 1000 }
        }).should.eql('followers > 1000');
        connector.buildWhere('Contact', {
            followers: { gte: 1000 }
        }).should.eql('followers >= 1000');
        connector.buildWhere('Contact', {
            followers: { lt: 1000 }
        }).should.eql('followers < 1000');
        connector.buildWhere('Contact', {
            followers: { lte: 1000 }
        }).should.eql('followers <= 1000');
        connector.buildWhere('Contact', {
            followers: { between: [100, 500] }
        }).should.eql('followers:100 TO 500');
        connector.buildWhere('Contact', {
            id: 1234
        }).should.eql('objectID:1234');
    });
        
    it('should create a complex search query', function() {
        connector.buildWhere('Contact', {
            or: [
                { state: 'OR' },
                { city: 'Anaheim' }
            ]
        }).should.eql('state:"OR" OR city:"Anaheim"');
        
        // Note: filter (X AND Y) OR Z is not allowed, only (X OR Y) AND Z is allowed
        
        connector.buildWhere('Contact', {
            and: [
                { state: 'OR' },
                { or: [{ city: 'Salem' }, { city: 'Eugene' }] }
            ]
        }).should.eql('state:"OR" AND (city:"Salem" OR city:"Eugene")');
        
        connector.buildWhere('Contact', {
            and: [
                { state: 'OR' },
                { city: { inq: ['Salem','Eugene'] } }
            ]
        }).should.eql('state:"OR" AND (city:"Salem" OR city:"Eugene")');
        
        connector.buildWhere('Contact', {
            and: [
                { state: 'OR' },
                { city: { neq: 'Salem' } }
            ]
        }).should.eql('state:"OR" AND NOT city:"Salem"');
        
        connector.buildWhere('Contact', {
            and: [
                { state: 'OR' },
                { city: { neq: 'Salem' } },
                { followers: { gt: 1000 } }
            ]
        }).should.eql('state:"OR" AND NOT city:"Salem" AND followers > 1000');
        
        connector.buildWhere('Contact', {
            and: [
                { id: 1234 },
                { city: { neq: 'Salem' } }
            ]
        }).should.eql('objectID:1234 AND NOT city:"Salem"');
        
        var filter = { where: { state: 'OR' } };
        mergeQuery(filter, { where: { city: { neq: 'Salem' } } });
        mergeQuery(filter, { where: { followers: { gt: 1000 } } });
        
        connector.buildWhere('Contact', filter.where)
            .should.eql('state:"OR" AND NOT city:"Salem" AND followers > 1000');
    });
    
    it('should serialize data using mapping, defaults and attributes', function() {
        connector.serializeData('Contact', _.extend({
            id: 'item-1234', unknown: 'ignored'
        }, contactData)).should.eql({
            objectID: 'item-1234',
            firstname: 'Fabien', lastname: 'Franzen',
            company: 'Atelier Fabien',
            city: 'Portland', state: 'OR',
            followers: 1327,
            registeredAt: 1490313600,
            _tags: ['foo', 'bar', 'baz']
        });
    });
    
    it('should normalize data using mapping', function() {
        connector.normalizeData('Contact', {
            objectID: 'item-1234',
            firstname: 'Fabien', lastname: 'Franzen',
            company: 'Atelier Fabien',
            city: 'Portland', state: 'OR',
            followers: 1327,
            registeredAt: 1490313600,
            _tags: ['foo', 'bar', 'baz'],
            unknown: 'ignored'
        }).should.eql(_.extend({
            id: 'item-1234'
        }, contactData));
    });
    
    it('should return the index settings for a model', function() {
        var settings = connector.getModelIndexSettings('Contact');
        settings.should.eql({
            searchableAttributes: [
                'firstname',
                'lastname',
                'company',
                'city',
                'state',
                'followers',
                'registeredAt'
            ],
            attributesForFaceting: ['state', 'city', 'followers', 'registeredAt'],
            unretrievableAttributes: ['isAdmin']
        });
    });
    
    it('should create a new entry', function(next) {
        Contact.create(contactData, { wait: true }, function(err, contact) {
            if (err) return next(err);
            contact.id.should.be.a.string;
            ids.contact = contact.id;
            contactData.id = contact.id;
            contactData.isAdmin = false;
            delete contactData.tags;
            next();
        });
    });
    
    it('should not create a duplicate entry - will overwrite existing', function(next) {
        Contact.create(contactData, { wait: true }, function(err, contact) {
            if (err) return next(err);
            contact.id.should.equal(ids.contact);
            next();
        });
    });
    
    it('should find an entry by id', function(next) {
        Contact.findById(ids.contact, function(err, contact) {
            if (err) return next(err);
            contact.should.be.instanceof(Contact);
            contact.toJSON().should.eql(contactData);
            next();
        });
    });
    
    it('should find an entry by id - where', function(next) {
        Contact.find({ where: { id: ids.contact } }, function(err, contacts) {
            contacts.should.have.length(1);
            contacts[0].toJSON().should.eql(contactData);
            next();
        });
    });
    
    it('should find multiple entries by id', function(next) {
        Contact.findByIds([ids.contact, 'xxx'], function(err, contacts) {
            contacts.should.have.length(1);
            contacts[0].should.be.instanceof(Contact);
            contacts[0].toJSON().should.eql(contactData);
            next();
        });
    });
    
    it('should find all entries', function(next) {
        var meta = {};
        Contact.find({
            limit: 5,
            fields: ['firstname', 'lastname']
        }, {
            indexName: 'dev_contacts', meta: meta
        }, function(err, contacts) {
            if (err) return next(err);
            contacts.should.be.an.array;
            contacts.should.have.length(5);
            
            contacts[0].should.be.instanceof(Contact);
            contacts[0].should.have.property('id');
            contacts[0].should.have.property('firstname');
            contacts[0].should.have.property('lastname');
            contacts[0].should.be.undefined;
            
            meta.nbHits.should.equal(500);
            meta.page.should.equal(0);
            meta.nbPages.should.equal(100);
            meta.hitsPerPage.should.equal(5);
            
           next();
        });
    });
    
    it('should find all entries - with facets', function(next) {
        var meta = {};
        Contact.find({
            offset: 5, limit: 5,
            facets: ['city', 'state'],
            fields: ['firstname', 'lastname'],
            maxValuesPerFacet: 10
        }, {
            indexName: 'dev_contacts', meta: meta
        }, function(err, contacts) {
            if (err) return next(err);
            contacts.should.be.an.array;
            contacts.should.not.be.empty;
            
            contacts[0].should.be.instanceof(Contact);
            contacts[0].should.have.property('id');
            contacts[0].should.have.property('firstname');
            contacts[0].should.have.property('lastname');
            contacts[0].should.be.undefined;
            
            meta.nbHits.should.equal(500);
            meta.page.should.equal(1);
            meta.nbPages.should.equal(100);
            meta.hitsPerPage.should.equal(5);
            
            meta.facets.should.be.an.object;
            meta.facets.should.eql({
                city: {
                    'Honolulu': 12,
                    'New York': 12,
                    'Anchorage': 9,
                    'Chicago': 9,
                    'Phoenix': 9,
                    'Los Angeles': 6,
                    'Philadelphia': 6,
                    'San Francisco': 6,
                    'Fairbanks': 5,
                    'Houston': 5
                },
                state: {
                    'CA': 74,
                    'PA': 36,
                    'TX': 34,
                    'NJ': 32,
                    'NY': 31,
                    'FL': 28,
                    'IL': 27,
                    'OH': 27,
                    'HI': 17,
                    'AK': 15
                }
            });
            
            next();
        });
    });
    
    it('should find all entries - query', function(next) {
        var meta = {};
        Contact.find({
            query: 'Jane',
            limit: 5,
            facets: ['city']
        }, {
            indexName: 'dev_contacts', meta: meta
        }, function(err, contacts) {
            if (err) return next(err);
            contacts.should.be.an.array;
            
            var expected = ['Janet', 'Janet', 'Betty Jane', 'Janice', 'Jade'];
            
            var names = _.pluck(contacts, 'firstname');
            names.should.eql(expected);
            
            meta.nbHits.should.equal(18);
            meta.page.should.equal(0);
            meta.nbPages.should.equal(4);
            meta.hitsPerPage.should.equal(5);
            
            meta.facets.should.be.an.object;
            meta.facets.should.eql({
                city: {
                    'Eugene': 3,
                    'Anchorage': 1,
                    'Branson': 1,
                    'Burlington': 1,
                    'Camarillo': 1,
                    'Centerburg': 1,
                    'Columbus': 1,
                    'Easton': 1,
                    'Grandview': 1,
                    'Hammond': 1,
                    'Hutchinson': 1,
                    'Kaneohe': 1,
                    'Loves Park': 1,
                    'Rialto': 1,
                    'Riverside': 1,
                    'San Jose': 1
                }
            });
            
            next();
        });
    });
    
    it('should find all entries - filtered', function(next) {
        var meta = {};
        Contact.find({
            where: { state: 'NY' },
            limit: 5,
            facets: ['city']
        }, {
            indexName: 'dev_contacts', meta: meta
        }, function(err, contacts) {
            if (err) return next(err);
            contacts.should.be.an.array;
            contacts.should.have.length(5);
            
            contacts[0].should.be.instanceof(Contact);
            contacts[0].should.have.property('id');
            contacts[0].should.have.property('firstname');
            contacts[0].should.have.property('lastname');
            
            _.all(contacts, function(contact) {
                return contact.state === 'NY';
            }).should.be.true;
            
            meta.nbHits.should.equal(31);
            meta.page.should.equal(0);
            meta.nbPages.should.equal(7);
            meta.hitsPerPage.should.equal(5);
            
            meta.facets.should.be.an.object;
            meta.facets.should.eql({
                city: {
                    'New York': 12,
                    'Brooklyn': 3,
                    'Middletown': 2,
                    'Attica': 1,
                    'Bedford Hills': 1,
                    'Bronx': 1,
                    'Buffalo': 1,
                    'East Aurora': 1,
                    'Fishkill': 1,
                    'Ithaca': 1,
                    'Manhasset': 1,
                    'Melville': 1,
                    'New Hyde Park': 1,
                    'Riverhead': 1,
                    'Rochester': 1,
                    'Shirley': 1,
                    'Warwick': 1
                }
            });

            next();
        });
    });
    
    it('should find all entries - complex', function(next) {
        var cities = ['Brooklyn', 'Middletown'];
        var meta = {};
        Contact.find({
            where: {
                state: 'NY',
                city: { inq: cities }
            },
            facets: ['city']
        }, {
            indexName: 'dev_contacts', meta: meta
        }, function(err, contacts) {
            if (err) return next(err);
            contacts.should.be.an.array;
            contacts.should.have.length(5);
            
            contacts[0].should.be.instanceof(Contact);
            contacts[0].should.have.property('id');
            contacts[0].should.have.property('firstname');
            contacts[0].should.have.property('lastname');
            
            _.all(contacts, function(contact) {
                return contact.state === 'NY' && _.include(cities, contact.city);
            }).should.be.true;
            
            meta.nbHits.should.equal(5);
            meta.page.should.equal(0);
            meta.nbPages.should.equal(1);
            meta.hitsPerPage.should.equal(20);
            
            meta.facets.should.be.an.object;
            meta.facets.should.eql({
                city: {
                    'Brooklyn': 3,
                    'Middletown': 2
                }
            });

            next();
        });
    });
    
    it('should check if an entry exists - raw', function(next) {
        connector.exists('Contact', ids.contact, {}, function(err, exists) {
            if (err) return next(err);
            exists.should.be.true;
            next();
        });
    });
    
    it('should check if an entry exists (1)', function(next) {
        Contact.exists(ids.contact, function(err, exists) {
            if (err) return next(err);
            exists.should.be.true;
            next();
        });
    });
    
    it('should check if an entry exists (2)', function(next) {
        Contact.exists('xxx', function(err, exists) {
            if (err) return next(err);
            exists.should.be.false;
            next();
        });
    });
    
    it('should count entries', function(next) {
        Contact.count({}, {
            indexName: 'dev_contacts'
        }, function(err, count) {
            if (err) return next(err);
            count.should.equal(500);
            next();
        });
    });
    
    it('should count entries - filtered (1)', function(next) {
        Contact.count({
            where: { state: 'NY' }
        }, {
            indexName: 'dev_contacts'
        }, function(err, count) {
            if (err) return next(err);
            count.should.equal(31);
            next();
        });
    });
    
    it('should count entries - filtered (2)', function(next) {
        Contact.count({
            where: { state: { inq: ['NY', 'OR'] } }
        }, {
            indexName: 'dev_contacts'
        }, function(err, count) {
            if (err) return next(err);
            count.should.equal(42);
            next();
        });
    });
    
    it('should update an entry: save', function(next) {
        Contact.findById(ids.contact, function(err, contact) {
            if (err) return next(err);
            contact.toJSON().should.eql(contactData);
            contact.city = 'Eugene',
            contactData.city = contact.city;
            contact.save(function(err, contact) {
                if (err) return next(err);
                contact.toJSON().should.eql(contactData);
                next();
            });
        });
    });
    
    it('should update an entry: updateAttributes', function(next) {
        Contact.findById(ids.contact, function(err, contact) {
            contact.id.should.equal(ids.contact);
            contact.toJSON().should.eql(contactData);
            
            contactData.city = 'New York';
            contactData.state = 'NY';
            
            contact.updateAttributes({
                city: 'New York', state: 'NY'
            }, function(err, user) {
                contact.toJSON().should.eql(contactData);
                next();
            });
        });
    });
    
    it('should have updated user', function(next) {
        Contact.findById(ids.contact, function(err, contact) {
            contact.id.should.equal(ids.contact);
            contact.toJSON().should.eql(contactData);
            next();
        });
    });
    
    it('should update multipe entries', function(next) {
        Contact.update({ state: 'NY' }, { city: 'Ithaca' }, function(err, result) {
            if (err) return next(err);
            result.should.eql({ count: 1 });
            contactData.city = 'Ithaca';
            next();
        });
    });
    
    it('should have updated multipe entries', function(next) {
        Contact.find({ state: 'NY' }, function(err, contacts) {
            contacts.should.be.an.array;
            contacts.should.have.length(1);
            contacts[0].id.should.equal(ids.contact);
            contacts[0].toJSON().should.eql(contactData);
            next();
        });
    });
    
    it('should delete entries (1)', function(next) {
        Contact.remove({ state: 'CA' }, function(err, result) {
            if (err) return next(err);
            result.should.eql({ count: 0 });
            next();
        });
    });
    
    it('should not have deleted any entries', function(next) {
        Contact.exists(ids.contact, function(err, exists) {
            exists.should.be.true;
            next();
        });
    });
    
    it('should delete entries (2)', function(next) {
        Contact.remove({ state: 'NY' }, function(err, result) {
            if (err) return next(err);
            result.should.eql({ count: 1 });
            next();
        });
    });
    
    it('should have deleted an entry', function(next) {
        Contact.exists(ids.contact, function(err, exists) {
            if (err) return next(err);
            exists.should.be.false;
            next();
        });
    });
    
    it('should have deleted all entries', function(next) {
        Contact.count(function(err, count) {
            if (err) return next(err);
            count.should.equal(0);
            next();
        });
    });
    
});
