-- Create raw market_data table, if does not exist
DROP TABLE IF EXISTS market_data;
CREATE TABLE market_data (
   id SERIAL PRIMARY KEY,
   stock TEXT,
   price INTEGER,
   event_timestamp TIMESTAMP
);

-- Create aggregate prices table, if does not exist
DROP TABLE IF EXISTS aggregate_price;
CREATE TABLE aggregate_price (
   stock TEXT,
   "Max Price" INTEGER,
   "Min Price" INTEGER,
   "Avg Price" FLOAT,
   "Time" TIMESTAMP
);