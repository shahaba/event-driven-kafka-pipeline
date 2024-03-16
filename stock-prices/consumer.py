import faust
from datetime import datetime
from producer import StockPrice
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

# Postgres Table Setup
DB_NAME = "market_data"
TABLE_NAME = "stock_prices"
BASE = declarative_base()


class StockPriceModel(BASE):
    """Sqlalchemy table model"""

    __tablename__ = TABLE_NAME
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock = Column(String)  # name of stock
    price = Column(Integer)
    event_timestamp = Column(DateTime)


# create database, if not already
engine = create_engine(f"postgresql://postgres@localhost:5432/{DB_NAME}")

# create database if it doesn't exist
if not database_exists(engine.url):
    create_database(engine.url)
    print(f"created database {DB_NAME}")

Session = sessionmaker(bind=engine)

app = faust.app("consume_stock_prices", boker="kafka://localhost:9092")

topic = app.topic("stock_prices", value_type=StockPrice)


# Consumer, waiting for new stock prices to be published
@app.agent(topic)
async def price(stocks):
    async for stock in stocks:
        print(f"{stock.stock} - {stock.price} - {stock.event_timestamp}")
        session = Session()

        # Try to send stock price to db
        try:
            db_stock = StockPriceModel(
                stock=stock.stock,
                price=stock.price,
                event_timestamp=stock.event_timestamp,
            )

            session.add(db_stock)
            session.commit()
        except Exception as e:
            # if there's an error trying to commit transaction
            print("Error:", e)
            # rollback transaction
            session.rollback()
        finally:
            session.close()


# start faust app
app.main()
