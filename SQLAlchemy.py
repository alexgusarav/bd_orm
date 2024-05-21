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


def load_json():
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


def getshops(publ):
    quer = session.query(
        Book.title, Shop.name, Sale.price, Sale.date_sale
        ).select_from(Shop).join(
        Stock, Stock.id_shop == Shop.id).join(
        Book, Book.id == Stock.id_book).join(
        Publisher, Publisher.id == Book.id_publisher).join(
        Sale, Stock.id == Sale.id_stock
        )
    if publ.isdigit():
        quer = quer.filter(Publisher.id == int(publ)).all()
    else:
        quer = quer.filter(Publisher.name == publ).all()
    for book, shop, price, data in quer:
        print(f"{book: <40} | {shop: <10} | {price: <8} | {data.strftime('%d-%m-%Y')}")


if __name__ == '__main__':
    DSN = "postgresql://postgres:12345@localhost:5432/neo_bd_02"
    engine = sqlalchemy.create_engine(DSN)
    Session = sessionmaker(bind=engine)
    session = Session()
    create_tables(engine)
    load_json()
    find_name = input("Введите автора или его id: ")
    getshops(find_name)
    session.close()
