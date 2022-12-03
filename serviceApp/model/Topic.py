from spyne import UnsignedInteger32, Unicode, Array

from db.dbInstance import TableModel, Permission


class Topic(TableModel):
    __tablename__ = 'topic'
    __namespace__ = 'spyne.inpoda.sql_crud'
    __table_args__ = {"sqlite_autoincrement": True}

    id = UnsignedInteger32(primary_key=True)
    topic = Unicode(256)
    tweet_id = Unicode(256)
    permissions = Array(Permission, store_as='table')
