import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from requests import get

FULLY_QUALIFIED_TABLE_NAME = "exchangeratesproject.rates.exchangerates"
TABLE_SCHEMA = [
    bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
    bigquery.SchemaField("USD_EUR", bigquery.enums.SqlTypeNames.FLOAT),
    bigquery.SchemaField("GBP_EUR", bigquery.enums.SqlTypeNames.FLOAT),
    bigquery.SchemaField("CHF_EUR", bigquery.enums.SqlTypeNames.FLOAT),
]
LAST_DATE_SQL_QUERY = f"select max(date) from {FULLY_QUALIFIED_TABLE_NAME}"

API_URL = "https://api.apilayer.com/exchangerates_data/timeseries"
API_KEY = os.environ.get("API_KEY")


def bigquery_get_last_date() -> datetime:
    """Get date of last inserted exchange rate.
    Returns:
        date of last stored exchange rate.

    """
    big_query_client = bigquery.Client()
    query_job = big_query_client.query(LAST_DATE_SQL_QUERY)
    result = query_job.result()
    last_date = list(result)[0].values()[0]
    if last_date is None:
        return datetime(2022, 5, 17)
    return last_date


def get_exchange_rates(
        start_date: str, end_date: str, base: str, symbols: str
) -> pd.DataFrame:
    """Get exchange rates from https://exchangeratesapi.io
    Args:
           start_date (str): the start date of your preferred timeframe,
           end_date (str): the end date of your preferred timeframe,
           base (str): the three-letter currency code of your preferred base currency,
           symbols (str): a list of comma-separated currency codes to limit output currencies.

    """
    # get data
    headers = {"apikey": API_KEY}
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "base": base,
        "symbols": symbols,
    }
    response = get(API_URL, headers=headers, params=params).json()

    # transform data
    exchange_rates = pd.DataFrame(response["rates"]).transpose()
    exchange_rates.reset_index(inplace=True)
    exchange_rates.rename(
        columns={"index": "date", "USD": "USD_EUR", "GBP": "GBP_EUR", "CHF": "CHF_EUR"},
        inplace=True,
    )
    exchange_rates["date"] = pd.to_datetime(exchange_rates["date"])
    return exchange_rates


def bigquery_insert(df: pd.DataFrame) -> None:
    """Insert into exchangerates table dataframe.
    Args:
           df (pd.DataFrame): values to insert.

    """
    job_config = bigquery.LoadJobConfig(schema=TABLE_SCHEMA)
    big_query_client = bigquery.Client()
    job = big_query_client.load_table_from_dataframe(
        dataframe=df,
        destination=FULLY_QUALIFIED_TABLE_NAME,
        job_config=job_config,
    )
    job.result()


if __name__ == "__main__":
    # define start_date and end_date for request
    from_date = (bigquery_get_last_date() + timedelta(days=1)).strftime("%Y-%m-%d")
    to_date = datetime.today().strftime("%Y-%m-%d")

    if to_date > from_date:
        rates_df = get_exchange_rates(
            start_date=from_date, end_date=to_date, base="EUR", symbols="USD,GBP,CHF"
        )
        bigquery_insert(rates_df)
        print(f"{len(rates_df)} records were inserted to the BigQuery table")
    else:
        print(f"The {FULLY_QUALIFIED_TABLE_NAME} table is already up-to-date")
