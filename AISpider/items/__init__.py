import typing
from scrapy import Item, Field


class BaseItem(Item):
    """
    from scrapy
    sample:
    class NewItem(BaseItem):
        field=Field()
    """
    metadata = Field()

    def __init__(self, metadata: typing.Optional[typing.Dict] = None, *args,
                 **kwargs):
        kwargs['metadata'] = metadata or {}
        super(BaseItem, self).__init__(*args, **kwargs)

    def get_table_name(self):
        """返回表名，用于数据存储，注意和数据库表名保持一致"""
        assert self.Meta is not None, "Item should contains innner class named Meta."
        return self.Meta.table

    def get_unique_fields(self):
        """返回用于确定记录唯一的字段，用于判断数据是否已存在，存在则自动更新(默认)，可以配合metedata修改默认操作"""
        assert self.Meta is not None, "Item should contains innner class named Meta."
        return self.Meta.unique_fields

    def get_save_fields(self):
        if not hasattr(self, 'Meta'):
            return []
        return (self.Meta.saved_fields or []) if hasattr(self.Meta, 'saved_fields') else []
