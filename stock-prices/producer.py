import faust
import random

from datetime import datetime


class StockPrice(faust.Record):
    """Define schema for StockPrice record

    Args:
        faust (Record): _description_
    """

    stock: str
    price: int
    event_timestamp: datetime


# Init Faust app
app = faust.App(
    "write_stock_prices",
    broker="kafka://localhost:9092",  # local kafka instance
)

topic = app.topic("stock_prices", value_type=StockPrice)

# init set of random stocks
stock = {
    "Apple": StockPrice(stock="Apple", price=50, event_timestamp=None),
    "Tesla": StockPrice(stock="Tesla", price=125, event_timestamp=None),
    "Google": StockPrice(stock="Google", price=75, event_timestamp=None),
    "Shopify": StockPrice(stock="Shopify", price=150, event_timestamp=None),
    "Netflix": StockPrice(stock="Netflix", price=200, event_timestamp=None),
}


# Producers, generate random stock price changes
@app.timer(2.0)
async def publish_prices(stocks):
    # Randomly update price value
    for name, stock in stocks.items():
        stock.price += random.randin(-5, 5)
        stock.event_timestamp = datetime.now()

        # debug record
        print(f"{stock.stock} - {stock.price} - {stock.event_timestamp}")

        await topic.send(value=stock)


# kickoff Faust app on call
app.main()
