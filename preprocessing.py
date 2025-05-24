"""
This script takes original data, DECENTRATHON_3.0.parquet, manipulate it
and will create a new data file, clustering_data.parquet that would be used for model training.
This file would also create a additional csv file that contains 
metadata of clustering_data.parquet
"""


import polars as pl

