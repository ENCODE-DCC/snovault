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
