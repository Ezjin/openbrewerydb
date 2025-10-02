import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa
import os
import pandas as pd

df1 = pd.read_parquet("./data_lake_mock/gold/batch=2025-10-02/total.parquet")


df2 = pd.read_parquet("./total_bck.parquet")


