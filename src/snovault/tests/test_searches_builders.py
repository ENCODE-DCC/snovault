import pytest


def test_searches_builders_view_builder_init():
    from snovault.searches.builders import ResponseBuilder
    rb = ResponseBuilder()
    assert isinstance(rb, ResponseBuilder)


def test_searchers_builders_view_builder_validate_response_fields():
    from snovault.searches.builders import ResponseBuilder
    from snovault.searches.fields import AbstractResponseField
    rf = AbstractResponseField()
    ResponseBuilder(response_fields=[rf])
    class NewResponseField(AbstractResponseField):
        def __init__(self):
            super().__init__()
    nrf = NewResponseField()
    ResponseBuilder(response_fields=[rf, nrf])
    class OtherResponseField():
        pass
    orf = OtherResponseField()
    with pytest.raises(ValueError):
        ResponseBuilder(response_fields=[rf, nrf, orf])
