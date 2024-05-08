from sqlalchemy import Column, String, Text, Integer, DateTime, func
from .metadata_base import Base


class ACT(Base):
    __tablename__ = 'act'

    id = Column(Integer, primary_key=True, autoincrement=True)
    da_number = Column(String(255), unique=True, nullable=False)
    address = Column(String(255),nullable=True, server_default=None)
    description = Column(Text,nullable=True, server_default=None)
    district = Column(String(255),nullable=True, server_default=None)
    suburb = Column(String(255),nullable=True, server_default=None)
    section = Column(String(255),nullable=True, server_default=None)
    block = Column(String(255),nullable=True, server_default=None)
    organisation = Column(String(255),nullable=True, server_default=None)
    stage = Column(String(255),nullable=True, server_default=None)
    lodgement_date = Column(Integer,nullable=True)
    start_date = Column(Integer,nullable=True,)
    end_date = Column(Integer,nullable=True,)
    application_amended = Column(String(255),nullable=True, server_default=None)
    documents = Column(Text,nullable=True, server_default=None)

    _tid = Column(String(255), nullable=False, server_default=None)
    created_at = Column(DateTime, server_default=func.now())
    update_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
