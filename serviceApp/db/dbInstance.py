import logging
from spyne import Unicode, TTableModel, UnsignedInteger32

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy.engine.base.Engine').setLevel(logging.DEBUG)

load_dotenv()

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
dbname = os.getenv('DB_DATABASE')


db = create_engine("postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, dbname))
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
