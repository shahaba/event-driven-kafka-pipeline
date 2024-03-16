import faust
import csv
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base

DB_NAME = "stock_ticker"
TABLE_NAME = "market_data"
BASE = declarative_base()

app = faust.App(
    "aggregator",
    broker="kafka://localhost:9092",
)


class StockPrice(faust.Record):
    stock: str
    price: int
    event_timestamp: int


topic = app.topic("stock_prices", value_type=StockPrice)

# create database, if not already
ENGINE = create_engine(f"postgresql://postgres@localhost:5432/{DB_NAME}")

agg_query = text(
    f"""
         select
            stock,
            MAX("Max Price") as "Max Price",
            MIN("Min Price") as "Min Price",
            AVG("Avg Price") as "Avg Price",
            MAX("Time") as "Time"
        from 
            aggregate_price
        group by 
            stock
        """
)


# Consumer, waiting for new stock prices to be published
@app.agent(topic)
async def price(stocks):
    with ENGINE.connect() as conn:
        write_aggregate(conn)
        write_csv(conn, agg_query)


@app.agent(topic)
async def write_prices_tenmin(stocks):
    await asyncio.sleep(600)
    with ENGINE.connect() as conn:
        write_market(conn)


def write_aggregate(conn):

    # Calculate the time 5 seconds ago
    time_now = datetime.now()
    time_5_seconds_ago = time_now - timedelta(seconds=5)

    conn.execute(
        text(
            f"""
        insert into aggregate_price
        select
            stock,
            MAX(price) as "Max Price",
            MIN(price) as "Min Price",
            AVG(price) as "Avg Price",
            MAX(event_timestamp) as "Time"
        from 
            market_data
        where
            event_timestamp between 
            '{ time_5_seconds_ago }'
            and '{ time_now }'
        group by 
            stock
        """
        )
    )

    conn.commit()


def write_market(conn):
    # Calculate the time 5 seconds ago
    time_now = datetime.now()
    time_10_mins_ago = time_now - timedelta(minutes=10)

    results = conn.execute(
        text(
            f"""
        select
            stock,
            MAX(price) as "Max Price",
            MIN(price) as "Min Price",
            AVG(price) as "Avg Price",
            MAX(event_timestamp) as "Time"
        from 
            market_data
        where
            event_timestamp between 
            '{ time_10_mins_ago }'
            and '{ time_now }'
        group by 
            stock
        """
        )
    )

    # Fetch all rows from the result set
    rows = results.fetchall()

    # Define the path for the CSV file
    csv_file_path = (
        f"tenmin/output_{ datetime.now().strftime('%Y-%m-%d_%H-%M-%S') }.csv"
    )

    # Write the results to a CSV file
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the header row
        csv_writer.writerow(results.keys())

        # Write each row of data
        csv_writer.writerows(rows)


def write_csv(conn, query):

    result = conn.execute(query)

    # Fetch all rows from the result set
    rows = result.fetchall()

    # Define the path for the CSV file
    csv_file_path = f"data/output_{ datetime.now().strftime('%Y-%m-%d_%H-%M') }.csv"

    # Write the results to a CSV file
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the header row
        csv_writer.writerow(result.keys())

        # Write each row of data
        csv_writer.writerows(rows)


app.main()
