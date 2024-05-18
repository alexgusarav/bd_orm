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

    def __str__(self):
        return f'{self.id_book, self.id_shop}'


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

# add

# pub1 = Publisher(id=1, name='O\u2019Reilly')
# pub2 = Publisher(id=2, name='Pearson')
# pub3 = Publisher(id=3, name='Microsoft Press')
# pub4 = Publisher(id=4, name='No starch press')
#
# session.add_all([pub1, pub2, pub3, pub4])
# session.commit()
#
# bk1 = Book(id=5, title='Modern Operating Systems', id_publisher=2)
# bk2 = Book(id=1, title='Modern Operating', id_publisher=1)
# sh1 = Shop(id=1, name='Labirint')
# st1 = Stock(id=1, id_shop=1, id_book=5, count=10)
# st2 = Stock(id=2, id_shop=1, id_book=1, count=5)
# sal1 = Sale(id=1, price=16, data_sale='2018-10-25T09:45:24.552Z', count=20, id_stock=1)
# sal2 = Sale(id=2, price=17, data_sale='2018-10-26T09:45:24.552Z', count=20, id_stock=2)
#
# session.add_all([bk1, bk2, sh1, st1, st2, sal1, sal2])
# session.commit()

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
    for sh in q1.all():
        for sal in q.all():
            print(f"{s.title}{(max_name - len(s.title)) * ' '} | "
                  f"{sh.name}{(max_shop - len(sh.name)) * ' '} | "
                  f"{sal.price}{(max_price - len(str(sal.price))) * ' '} | "
                  f"{str(sal.date_sale)[:10]}")

session.close()
