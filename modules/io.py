import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def write_json(content, filename):
    with open(filename, 'w', encoding = "UTF-8") as f:
        json.dump(content, f)

def read_json(filename):
    with open(filename) as f:
        return json.load(f)
    
def into_parquet(dict_data, filename):
    # Convert dictionary into DataFrame
    df = pd.DataFrame.from_dict(dict_data)
    # Fix datetime datatype
    df['recorded_at'] = df['recorded_at'].astype('datetime64[ms]')
    df['issued_on'] = df['issued_on'].astype('datetime64[ms]')

    # Save into Parquet file
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{filename}.parquet")