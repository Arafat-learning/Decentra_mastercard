"""
This script takes original data, DECENTRATHON_3.0.parquet, manipulate it
and will create a new data file, clustering_data.parquet that would be used for model training.
This file would also create a additional csv file that contains 
metadata of clustering_data.parquet
"""

# polars would be used for manipulating data
import polars as pl

# reading the original data
data = pl.read_parquet("data/DECENTRATHON_3.0.parquet")


### Creating clustering_data

# to create a reference for last trancactions. Jan 1, 2025 when data collection stops.
reference_date = pl.lit("2025-01-01").str.to_date()

# creating aggregate value columns
clustering_data = data.group_by("card_id").agg(
    (reference_date - pl.col("transaction_timestamp").max()).dt.total_hours().alias("last_trans"),
    (pl.col("transaction_timestamp").max() - pl.col("transaction_timestamp").min()).dt.total_days().alias("use_dur"),
    (pl.col("transaction_amount_kzt").sum()).alias("total_trans"),
    (pl.col("transaction_amount_kzt").count()).alias("number_trans"),
    (pl.col("transaction_amount_kzt").mean()).alias("avg_trans")
).with_columns(
    daily_turnover = pl.col("number_trans")/pl.col("use_dur")
)
# adding dictionary
descriptions = [
    ("last_trans", "The number of hours since last transaction"),
    ("use_dur", "The number of days customer used card. Days between first tranaction and last"),
    ("total_trans", "Total amount of money used in transactions in KZT"),
    ("number_trans", "The number of transactions customer performed"),
    ("avg_trans", "Average amount of money used per transaction"),
    ("daily_turnover", "Average number of transaction per day")
]
# initializing data_dictionary.csv
dictionary = pl.DataFrame(
    descriptions,
    schema={
        "column_name": pl.String,
        "description": pl.String
        },
    orient="row"
)

## Column Groups: total transaction by value in column
def add_column_group(clustering_data, column, name, null_des = None):
    """
    This function will create dataframe that describes how much customer have spend on transaction
    per type of transaction. It will also add description to data_description
    """

    # creating a dataframe by merchant category total tranactions
    column_transactions = data.pivot(
        values="transaction_amount_kzt",
        index="card_id",
        on=column,
        aggregate_function="sum").fill_null(0).rename(lambda c: f"{name}_{c}" if c !="card_id" else c)

    # adding description to dictionary
    if null_des:
        types = [f"{name}_{c}" for c in data.get_column(column).unique()]
        descriptions = [f"total transactions by {type_}" if type_ else f"total transactions by {null_des}" for type_ in types]
        dictionary.vstack(pl.DataFrame({"column_name": types,
                                        "description": descriptions}),
                        in_place=True)
    else:
        types = [f"{name}_{c}" for c in data.get_column(column).unique()]
        descriptions = [f"total transactions by {type_}" for type_ in types]
        dictionary.vstack(pl.DataFrame({"column_name": types,
                                        "description": descriptions}),
                        in_place=True)
        
    # joining the column transactions
    return clustering_data.join(column_transactions, on="card_id", how="left")

## Adding columns to clustering data depending on how much money used in different types of transactions

# pos entry mode
clustering_data = add_column_group(clustering_data, "pos_entry_mode", "pos", "no pos transaction")

# transaction type
clustering_data = add_column_group(clustering_data, "transaction_type", "type")

# merchant category
clustering_data = add_column_group(clustering_data, "mcc_category", "category")

# wallet type
clustering_data = add_column_group(clustering_data, "wallet_type", "wallet", "regular_card")

# tranaction currency
clustering_data = add_column_group(clustering_data, "transaction_currency", "in")


### Writing data files to data directory
clustering_data.write_parquet("data/clustering_data.parquet")
dictionary.write_csv("data/data_dictionary.csv")
