import pytest
from unittest.mock import Mock


class TestPickStorage(object):

    def test_get_rev_links_with_switched_storage(self):
        # If we have a model that was loaded from elasticsearch
        # but the datastore is now database, we need to be sure
        # to get the rev links from the database.
        from pyramid.testing import (
            DummyRequest,
            testConfig,
        )
        from ..esstorage import (
            CachedModel,
            PickStorage,
        )

        model = CachedModel({'_source': {'uuid': 'dummy'}})
        model2 = Mock()
        read = Mock()
        write = Mock()
        write.get_by_uuid.return_value = model2
        storage = PickStorage(read, write)

        request = DummyRequest()
        request.datastore = 'database'
        with testConfig(request=request):
            storage.get_rev_links(model, 'rel')

        assert write.get_rev_links.called_once
        rev_links_model = write.get_rev_links.call_args[0][0]
        assert rev_links_model is model2


def test_pick_storage_get_by_unique_key_forces_database_request_when_model_is_invalidated(dummy_request):
    from pyramid.testing import testConfig
    from ..esstorage import PickStorage
    # Uses ES when model not invalidated.
    dummy_request.datastore = 'elasticsearch'
    es_model = Mock()
    pg_model = Mock()
    read = Mock()
    write = Mock()
    read.get_by_unique_key.return_value = es_model
    write.get_by_unique_key.return_value = pg_model
    es_model.invalidated.return_value = False
    pg_model.invalidated.return_value = False
    storage = PickStorage(read, write)
    with testConfig(request=dummy_request):
        model = storage.get_by_unique_key('accession', 'SNOFL000LSQ')
    assert model is es_model
    assert dummy_request.datastore == 'elasticsearch'
    # Switches to PG when model invalidated.
    es_model.invalidated.return_value = True
    with testConfig(request=dummy_request):
        model = storage.get_by_unique_key('accession', 'SNOFL000LSQ')
    assert model is pg_model
    assert dummy_request.datastore == 'database'


def test_pick_storage_get_by_uuid_forces_database_request_when_model_is_invalidated(dummy_request):
    from pyramid.testing import testConfig
    from ..esstorage import PickStorage
    # Uses ES when model not invalidated.
    dummy_request.datastore = 'elasticsearch'
    es_model = Mock()
    pg_model = Mock()
    read = Mock()
    write = Mock()
    read.get_by_uuid.return_value = es_model
    write.get_by_uuid.return_value = pg_model
    es_model.invalidated.return_value = False
    pg_model.invalidated.return_value = False
    storage = PickStorage(read, write)
    with testConfig(request=dummy_request):
        model = storage.get_by_uuid('123')
    assert model is es_model
    assert dummy_request.datastore == 'elasticsearch'
    # Switches to PG when model invalidated.
    es_model.invalidated.return_value = True
    with testConfig(request=dummy_request):
        model = storage.get_by_uuid('123')
    assert model is pg_model
    assert dummy_request.datastore == 'database'


def test_pick_storage_get_by_unique_key_does_not_force_database_when_model_is_none(dummy_request):
    from pyramid.testing import testConfig
    from ..esstorage import PickStorage
    dummy_request.datastore = 'elasticsearch'
    es_model = Mock()
    pg_model = Mock()
    read = Mock()
    write = Mock()
    read.get_by_unique_key.return_value = es_model
    write.get_by_unique_key.return_value = pg_model
    es_model.invalidated.return_value = False
    pg_model.invalidated.return_value = False
    storage = PickStorage(read, write)
    with testConfig(request=dummy_request):
        model = storage.get_by_unique_key('accession', 'SNOFL000LSQ')
    assert model is es_model
    assert dummy_request.datastore == 'elasticsearch'
    # Set model to None.
    read.get_by_unique_key.return_value = None
    with testConfig(request=dummy_request):
        model = storage.get_by_unique_key('accession', 'SNOFL000LSQ')
    # Make sure it looks in PG and doesn't force request.
    assert model is pg_model
    assert dummy_request.datastore == 'elasticsearch'


def test_pick_storage_get_by_uuid_does_not_force_database_when_model_is_none(dummy_request):
    from pyramid.testing import testConfig
    from ..esstorage import PickStorage
    dummy_request.datastore = 'elasticsearch'
    es_model = Mock()
    pg_model = Mock()
    read = Mock()
    write = Mock()
    read.get_by_uuid.return_value = es_model
    write.get_by_uuid.return_value = pg_model
    es_model.invalidated.return_value = False
    pg_model.invalidated.return_value = False
    storage = PickStorage(read, write)
    with testConfig(request=dummy_request):
        model = storage.get_by_uuid('123')
    assert model is es_model
    assert dummy_request.datastore == 'elasticsearch'
    # Set model to None.
    read.get_by_uuid.return_value = None
    with testConfig(request=dummy_request):
        model = storage.get_by_uuid('123')
    # Make sure it looks in PG and doesn't force request.
    assert model is pg_model
    assert dummy_request.datastore == 'elasticsearch'
