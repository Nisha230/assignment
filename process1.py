import pandas as pd
import os
import argparse

def read_parquet(file_path):
    return pd.read_parquet(file_path)

def split_trips(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(['unit', 'timestamp'])
    df['trip_id'] = (df.groupby('unit')['timestamp']
                     .diff()
                     .fillna(pd.Timedelta(seconds=0))
                     .gt('7H')
                     .cumsum())
    return df


def save_trips(df, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for unit, unit_df in df.groupby('unit'):
        for trip_id, trip_df in unit_df.groupby('trip_id'):
            # Format the timestamp column
            trip_df['timestamp'] = trip_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
            file_name = f"{unit}_{trip_id}.csv"
            trip_df[['latitude', 'longitude', 'timestamp']].to_csv(os.path.join(output_dir, file_name), index=False)

def process_gps_data(parquet_file, output_dir):
    df = read_parquet(parquet_file)
    trips_df = split_trips(df)
    save_trips(trips_df, output_dir)

def main():
    parser = argparse.ArgumentParser(description='Process GPS data to extract trips.')
    parser.add_argument('--to_process', required=True, help='Path to the Parquet file to be processed.')
    parser.add_argument('--output_dir', required=True, help='The folder to store the resulting CSV files.')
    
    args = parser.parse_args()

    process_gps_data(args.to_process, args.output_dir)

if __name__ == "__main__":
    main()