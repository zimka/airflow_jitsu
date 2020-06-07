import datetime
from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text,\
                       create_engine, UniqueConstraint, Boolean
from sqlalchemy.orm import sessionmaker, scoped_session, relationship, exc as exceptions
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSON
from sqlalchemy.dialects import postgresql

Base = declarative_base()


class OrderDataset(Base):
    __tablename__ = "dataset"

    uuid = Column('uuid', UUID(as_uuid=True), primary_key=True)
    name = Column('name', String())
    age = Column('age', Integer())
    good_title = Column('good_title', String())
    date = Column('date', DateTime())
    payment_status = Column('payment_status', Boolean())
    total_price = Column('total_price', Float(precision=2, asdecimal=True)) #rounding!
    amount = Column('amount', Integer())
    last_modified_at = Column('last_modified_at', DateTime(), default=datetime.datetime.now)

    @classmethod
    def get_toy_engine(cls):
        sqa_toy_string = "postgresql+psycopg2://toy:toy@127.0.0.1:5432/sandbox"
        return create_engine(sqa_toy_string)

    @classmethod
    def dump_dataframe(cls, dataset_df, engine=None):
        if engine is None:
            engine = cls.get_toy_engine()

        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        values = [
            data.to_dict() for num, data
            in dataset_df.reset_index().iterrows()
        ]
        cls.bulk_upsert(session, values)
        session.close()

    @classmethod
    def bulk_upsert(cls, session, values):
        stmt = postgresql.insert(cls.__table__).values(values)
        replace_set_ = {
            'payment_status': stmt.excluded.payment_status
        }
        pk = cls.__table__.primary_key
        stmt = stmt.on_conflict_do_update(
            constraint=pk,
            set_=replace_set_,
        )
        session.execute(
            stmt
        )
        session.commit()
