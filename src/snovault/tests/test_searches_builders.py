import pytest


def test_searches_builders_view_builder_init():
    from snovault.searches.builders import ViewBuilder
    vb = ViewBuilder()
    assert isinstance(vb, ViewBuilder)


def test_searchers_builders_view_builder_validate_response_fields():
    from snovault.searches.builders import ViewBuilder
    from snovault.searches.fields import AbstractResponseField
    rf = AbstractResponseField()
    vb = ViewBuilder(response_fields=[rf])
    class NewResponseField(AbstractResponseField):
        def __init__(self):
            super().__init__()
    nrf = NewResponseField()
    vb = ViewBuilder(response_fields=[rf, nrf])
    class OtherResponseField():
        pass
    orf = OtherResponseField()
    with pytest.raises(ValueError):
        vb = ViewBuilder(response_fields=[rf, nrf, orf])
