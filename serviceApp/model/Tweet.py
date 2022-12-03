import sqlalchemy
from spyne import UnsignedInteger32, Unicode, Array

from db.dbInstance import TableModel, Permission
from model import User


class Tweet(TableModel):
    __tablename__ = 'tweet'
    __namespace__ = 'spyne.inpoda.sql_crud'
    __table_args__ = {"sqlite_autoincrement": True}
    pk = UnsignedInteger32(primary_key=True)
    id = Unicode(256)
    text = Unicode(2048, db_type=sqlalchemy.UnicodeText)
    author_id = Unicode(256)
    topic = Unicode(256, db_type=sqlalchemy.UnicodeText)
    permissions = Array(Permission, store_as='table')
