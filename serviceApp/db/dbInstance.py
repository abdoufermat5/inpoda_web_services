import logging
from spyne import Unicode, TTableModel, UnsignedInteger32

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy.engine.base.Engine').setLevel(logging.DEBUG)


db = create_engine('sqlite:///inPoda.db')
Session = sessionmaker(bind=db)
TableModel = TTableModel()
TableModel.Attributes.sqla_metadata.bind = db


class Permission(TableModel):
    __tablename__ = 'permission'
    __namespace__ = 'spyne.inpoda.sql_crud'
    __table_args__ = {"sqlite_autoincrement": True}

    id = UnsignedInteger32(primary_key=True)
    application = Unicode(256)
    operation = Unicode(256)
