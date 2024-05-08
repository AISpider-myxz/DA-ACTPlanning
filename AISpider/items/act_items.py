from . import BaseItem
from scrapy import Field


class ACTItem(BaseItem):
    da_number = Field()
    address = Field()
    description = Field()
    district = Field()
    suburb = Field()
    section = Field()
    block = Field()
    organisation = Field()
    stage = Field()
    lodgement_date = Field()
    start_date = Field()
    end_date = Field()
    application_amended = Field()
    documents = Field()

    class Meta:
        table = 'act'
        unique_fields = ['da_number']
        saved_fields = ['documents']
