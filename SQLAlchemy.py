import sqlalchemy
import json
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40))
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="book")


class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship(Book, backref="id_book")
    shop = relationship(Shop, backref="id_shop")


class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship(Stock, backref="id_stock")


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


DSN = "postgresql://postgres:12345@localhost:5432/neo_bd_02"
engine = sqlalchemy.create_engine(DSN)

Session = sessionmaker(bind=engine)
session = Session()

create_tables(engine)

with open('fixtures/tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

# find_name = input()
find_name = 'O\u2019Reilly'

subq = session.query(Publisher).filter(Publisher.name == find_name).subquery()
q3 = session.query(Book).join(subq, Book.id_publisher == subq.c.id)
q3s = q3.subquery()
q2 = session.query(Stock).join(q3s, Stock.id_book == q3s.c.id)
q2s = q2.subquery()
q1 = session.query(Shop).join(q2s, Shop.id == q2s.c.id_shop)
q = session.query(Sale).join(q2s, Sale.id_stock == q2s.c.id)

max_name = max(len(s.title) for s in q3.all())
max_shop = max(len(sh.name) for sh in q1.all())
max_price = max(len(str(sal.price)) for sal in q.all())

for s in q3.all():
    for st in q2.all():
        for sh in q1.all():
            for sal in q.all():
                if sal.id_stock == st.id and st.id_shop == sh.id and st.id_book == s.id:
                    print(f"{s.title}{(max_name - len(s.title)) * ' '} | "
                          f"{sh.name}{(max_shop - len(sh.name)) * ' '} | "
                          f"{sal.price}{(max_price - len(str(sal.price))) * ' '} | "
                          f"{str(sal.date_sale)[:10]}")

session.close()
