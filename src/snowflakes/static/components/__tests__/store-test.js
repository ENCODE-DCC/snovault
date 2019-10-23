'use strict';

jest.autoMockOff();

// Fixes https://github.com/facebook/jest/issues/78
jest.dontMock('react');
jest.dontMock('underscore');

require('whatwg-fetch');


describe('ItemStore', function() {
    var ItemStore, items, view, fetch;

    beforeEach(function() {
        ItemStore = require('../lib/store').ItemStore;
        fetch = jest.fn();
        fetch.mockResponse = function(data, options) {
            this.mockReturnValue(Promise.resolve(new Response(JSON.stringify(data), options || {status: 200})));
        };
        items = [{'@id': '/items/foo/'}];
        view = {
            context: {fetch: fetch},
            setState: jest.fn(),
        };
    });

    describe('Instantiating a store', function() {
        var store;

        beforeEach(function() {
            store = new ItemStore(items, view, 'items');
        });

        it('holds the initial items', function() {
            expect(store._items.length).toEqual(1);
        });
    });

    describe('Creating an item', function() {
        var store, done;

        beforeEach(function() {
            view.onCreate = jest.fn();
            fetch.mockResponse({
                '@graph': [{'@id': '/items/bar/'}]
            });
            
            store = new ItemStore(items, view, 'items');
            done = store.create('/items/', {'@id': 'bar'});
        });

        it('posts the item to the backend', function() {
            done.then(() => expect(fetch.mock.calls.length).toEqual(1));
        });

        it('updates the internal list of items', function() {
            done.then(() => expect(store._items.length).toEqual(2));
        });

        it('calls the view onCreate callback with the response', function() {
            done.then(() => {
                expect(view.onCreate).toBeCalledWith({'@graph': [{'@id': '/items/bar/'}]});
            });
        });

        it('notifies the view of its new state', function() {
            done.then(() => expect(view.setState.mock.calls[0][0].items.length).toEqual(2));
        });
    });

    describe('Updating an item', function() {
        var store, done;

        beforeEach(function() {
            view.onUpdate = jest.fn();
            fetch.mockResponse({});
            
            store = new ItemStore(items, view, 'items');
            done = store.update({'@id': '/items/foo/', updated: true});
        });

        it('puts the item to the backend', function() {
            done.then(function() {
                expect(fetch.mock.calls.length).toEqual(1);
                var args = fetch.mock.calls[0];
                expect(args[1].method).toEqual('PUT');
            });
        });

        it('updates the internal list of items', function() {
            return done.then(() => expect(store._items[0].updated).toBe(true));
        });

        it('calls the view onUpdate callback with the response', function() {
            done.then(() => {
                expect(view.onUpdate).toBeCalledWith({'@id': '/items/foo/', updated: true});
            });
        });
    });

    describe('Deleting an item', function() {
        var store, done;

        beforeEach(function() {
            view.onDelete = jest.fn();
            fetch.mockResponse({});
            
            store = new ItemStore(items, view, 'items');
            done = store.delete('/items/foo/');
        });

        it('updates the status on the backend', function() {
            return done.then(function() {
                expect(fetch.mock.calls.length).toEqual(1);
                var args = fetch.mock.calls[0];
                expect(args[1].method).toEqual('PATCH');
                var body = JSON.parse(args[1].body);
                expect(body).toEqual({status: 'deleted'});
            });
        });

        it('updates the internal list of items', function() {
            return done.then(() => expect(store._items.length).toEqual(0));
        });

        it('calls the view onDelete callback with the removed item', function() {
            return done.then(() => {
                expect(view.onDelete).toBeCalledWith({'@id': '/items/foo/'});
            });
        });
    });

    describe('Reporting errors', function() {
        it('calls the view onError callback in case of HTTP errors', function() {
            view.onError = jest.fn();
            fetch.mockResponse({message: 'failure'}, {status: 500, headers: new Headers({'Content-Type': 'application/json'})});

            var store = new ItemStore(items, view, 'items');
            store.create('/items/', {}).then(function() {
                expect(view.onError.mock.calls[0][0].message).toEqual('failure');
            });
        });
    });

});
