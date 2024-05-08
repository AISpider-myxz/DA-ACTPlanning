import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

pymysql.install_as_MySQLdb()


class DBConnection():
    def __new__(cls, *args, **kwargs):
        cls = super().__new__(cls, *args, **kwargs)
        uri = 'mysql+mysqldb://root:discord@172.25.208.1/spider?charset=utf8mb4'
        engine = create_engine(uri, echo=True)
        Session = sessionmaker(bind=engine)
        cls.session = Session()
        return cls


session = DBConnection().session
