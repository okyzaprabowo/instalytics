"""Prepare data for Plotly Dash."""
import pandas as pd
import numpy as np


def instalytics_dataframe():
    """DataFrame from local tbl_scraping.csv"""
    df = pd.read_csv('data/tbl_scraping_skpd.csv')
    df['taken_at'] = pd.to_datetime(df['taken_at']).dt.date
    df['taken_at'] = pd.to_datetime(df['taken_at'])
    return df