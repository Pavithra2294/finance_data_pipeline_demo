CREATE SCHEMA IF NOT EXISTS financial_data

CREATE TABLE finance_data.staging_transactions(
    ID VARCHAR(20),
    Segment VARCHAR(50),
    Country VARCHAR(50),
    Product VARCHAR(100),
    Units_Sold INTEGER,
    Manufacturing_Price DECIMAL(10,2),
    Sale_Price DECIMAL(10,2),
    Gross_Sales DECIMAL(12,2),
    Discounts DECIMAL(12,2),
    Sales DECIMAL(12,2),
    COGS DECIMAL(12,2),
    Profit DECIMAL(12,2),
    Date DATE,
    Year INTEGER
);

COPY finance_data
FROM 's3://finance-datapav/destination-refined/sample_financial_data_100_extended.csv'
IAM_ROLE 'arn:aws:iam::your-account-id:role/your-redshift-role'
CSV
IGNOREHEADER 1
DATEFORMAT 'DD-MM-YYYY';

CREATE TABLE IF NOT EXISTS financial_data.daily_summary AS
SELECT
    transaction_date,
    category,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM financial_data.staging_transactions
GROUP BY transaction_date, category;

Optional: Optimize performance
VACUUM financial_data.staging_transactions;
ANALYZE financial_data.staging_transactions;



------------------arn details:
C:\Users\ashok\tf_demo>aws sts get-caller-identity --region eu-north-1
{
    "UserId": "AIDAVAHL63CWWMAET5NCV",
    "Account": "344092235949",
    "Arn": "arn:aws:iam::344092235949:user/pav-demo"
}

