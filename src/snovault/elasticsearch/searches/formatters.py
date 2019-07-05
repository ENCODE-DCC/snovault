from .interfaces import APPENDED
from .interfaces import FIELD_KEY
from .interfaces import TERMS
from .interfaces import TITLE
from .interfaces import TOTAL
from .interfaces import TYPE_KEY



class AggregationFormatter():

    def __init__(self):
        pass

    def _get_aggregation_field():
         pass


    def _get_aggregation_title():
        pass


    def _get_aggregation_type():
        pass


    def _get_aggregation_results():
        pass


    def _get_aggregation_total():
        pass


    def _aggregation_is_appeneded():
        pass


    def _format_aggregation():
        return {
            FIELD_KEY: get_aggregation_field(),
            TITLE: get_aggregation_title(),
            TERMS: get_aggregation_results(),
            TOTAL: get_aggregation_total(),
            TYPE_KEY: get_aggregation_type(),
            APPENDED: aggregation_is_appeneded(),
        }


    def format_aggregations():
        pass
