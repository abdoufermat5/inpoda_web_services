from spyne import UnsignedInteger32, Unicode, Array, Float

from db.dbInstance import TableModel, Permission


class Sentiment(TableModel):
    __tablename__ = 'sentiment'
    __namespace__ = 'spyne.inpoda.sql_crud'
    __table_args__ = {"sqlite_autoincrement": True}

    id = UnsignedInteger32(primary_key=True)
    sentiment = Unicode(256)
    polarity = Float()
    subjectivity = Float()
    tweet_id = Unicode(256)
    permissions = Array(Permission, store_as='table')
