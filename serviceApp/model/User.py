from spyne import UnsignedInteger32, Unicode, Array

from db.dbInstance import TableModel, Permission


class User(TableModel):
    __tablename__ = 'user'
    __namespace__ = 'spyne.inpoda.sql_crud'
    __table_args__ = {"sqlite_autoincrement": True}

    id = UnsignedInteger32(primary_key=True)
    name = Unicode(256)
    first_name = Unicode(256)
    last_name = Unicode(256)
    permissions = Array(Permission, store_as='table')