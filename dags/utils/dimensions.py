import pandas as pd
from pathlib import Path

def update_dimension(df, colname, filepath):
    new_df = df

    if Path(filepath).exists():
        old_df = pd.read_parquet(filepath)
        combined = pd.concat([old_df, new_df]).drop_duplicates().sort_values(colname)
    else:
        combined = new_df

    combined.to_parquet(filepath, index=False)
