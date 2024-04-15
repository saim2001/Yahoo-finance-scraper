import pandas as pd


def read_csv(file):
    df = pd.read_excel(file)
    return df