-- Get the total number of entries, avg price, and last timestamp
SELECT 
	stock, 
	COUNT(id) as num_records, 
	ROUND(AVG(price), 2) as avg_price, 
	MAX(event_timestamp::date) as last_timestamp
FROM 
	stock_prices
group by stock
order by 1;

-- Get max price by date
SELECT 
	stock, 
	event_timestamp::date as date, 
	MAX(price) as max_price
FROM 
	stock_prices
group by 1, 2
order by 1, 2;