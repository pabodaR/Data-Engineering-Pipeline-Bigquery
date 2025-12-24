CREATE TABLE IF NOT EXISTS analytics.financials(
	TransactionID STRING,
	Region STRING,
	Country STRING,
	Product STRING,
	Date DATE,
	Currency STRING,
	Revenue FLOAT64,
	Expense FLOAT64,
	Profit FLOAT64
);
