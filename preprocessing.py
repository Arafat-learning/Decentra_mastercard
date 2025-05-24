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

# initializing data_dictionary.csv
dictionary = pl.DataFrame(
    schema={
        "column_name": pl.String,
        "description": pl.String
    }
)


### Creating clustering_data

# to create a reference for last and first trancactions. Jan 1, 2025 when data collection stops.
reference_date = pl.lit("2025-01-01").str.to_date()

## Column Group: total transaction by pos entry mode
# creating a dataframe by pos_entry_mode total tranactions
pos_transactions = data.pivot(
    values="transaction_amount_kzt",
    index="card_id",
    on="pos_entry_mode",
    aggregate_function="sum")

# adding description to dictionary
pos_types = data.get_column("pos_entry_mode").unique()
pos_descriptions = [f"total transactions by {pos_type}" for pos_type in pos_types]
dictionary.vstack(pl.DataFrame({"column_name": pos_types,
                                "description": pos_descriptions}),
                  in_place=True)

### Writing data files to data directory
pos_transactions.write_parquet("data/clustering_data.parquet")
dictionary.write_csv("data/data_dictionary.csv")
