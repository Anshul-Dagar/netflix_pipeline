import dlt
import pandas as pd
import requests
from io import BytesIO

# Define constants
EXCEL_URL = 'https://www.netflix.com/tudum/top10/data/all-weeks-global.xlsx'
SUPABASE_CONNECTION_STRING = 'postgresql://postgres.dpmlropwzmetnnkpbgmh:CMTFX510HWWPseCS@aws-0-us-east-1.pooler.supabase.com:5432/postgres'
SUPABASE_TABLE = 'netflix_top10'

def download_excel(url):
    response = requests.get(url)
    response.raise_for_status()  # Ensure we notice bad responses
    return BytesIO(response.content)

def process_excel(file):
    df = pd.read_excel(file)
    return df

@dlt.resource(name='netflix_top10_data')
def extract_data():
    excel_file = download_excel(EXCEL_URL)
    df = process_excel(excel_file)
    for record in df.to_dict(orient='records'):
        yield record

def main():
    pipeline = dlt.pipeline(
        pipeline_name='netflix_top10_pipeline',
        destination=dlt.destinations.postgres("postgres://postgres.dpmlropwzmetnnkpbgmh:CMTFX51oHWWPseCS@aws-0-us-east-1.pooler.supabase.com:5432/postgres"),
        dataset_name="netflix_top10"
    )

    data = extract_data()
    info = pipeline.run(data, table_name="netflix_top10", write_disposition="replace")
    print(info)

if __name__ == "__main__":
    main()

