import pandas as pd
from datetime import datetime

def categorize_spending(amount):
    if amount < 5:
        return "Low"
    elif amount < 8:
        return "Medium"
    else:
        return "High"

def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    #standardize columns
    df.columns = df.columns.str.lower().str.strip().str.replace(" ","_")

    #cnvert into datetime 
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    #
    df["quantity"] = df["quantity"].astype(int)
    df["price_per_unit"] = df["price_per_unit"].astype(float)

    #
    df["total_spent"] = df["quantity"]*df["price_per_unit"]
    #business flag
    threshold = df["total_spent"].quantile(0.80)
    df["is_high_value_order"] = df["total_spent"] >= threshold

    df["spending_category"] = df["total_spent"].apply(categorize_spending)

    #normalize location
    df["location"] = df["location"].replace(["UNKNOWN","ERROR"], "Other")

    #normalize payment_method
    df["payment_method"] = df["payment_method"].replace(["UNKNOWN","ERROR"],"Other")

    #Meta deta
    df["processed_timestamp"] = datetime.now()

    return df