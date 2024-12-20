CREATE TABLE table_m3(
	name VARCHAR(500),
	releaseDate VARCHAR(50),
	copiesSold INTEGER,
	price FLOAT,
	revenue FLOAT,
	avgPlaytime FLOAT,
	reviewScore INTEGER,
	publisherClass VARCHAR(500),
	publishers VARCHAR(500),
	developers VARCHAR(500),
	steamId INT PRIMARY KEY
);

COPY table_m3(name, releaseDate, copiesSold, price, revenue, avgPlaytime, reviewScore, publisherClass, publishers, developers, steamid)
FROM '/tmp/P2M3_catherine-kezia_data_raw.csv'
DELIMITER ','
CSV HEADER
;