import os
import requests
import argparse
import json
from concurrent.futures import ThreadPoolExecutor
import concurrent

# Load API key and URL from environment variables or a configuration file
TOLLGURU_API_KEY = os.getenv("TOLLGURU_API_KEY")
TOLLGURU_API_URL = os.getenv("TOLLGURU_API_URL")

def send_file_to_api(file_path):
    headers = {'x-api-key': TOLLGURU_API_KEY, 'Content-Type': 'text/csv'}
    params = {'mapProvider': 'osrm', 'vehicleType': '5AxlesTruck'}

    with open(file_path, 'rb') as file:
        response = requests.post(TOLLGURU_API_URL, params=params, headers=headers, data=file)
        return response.json(), file_path

def save_json_response(response_data, output_dir, file_name):
    output_file = os.path.join(output_dir, file_name.replace('.csv', '.json'))
    with open(output_file, 'w') as f:
        json.dump(response_data, f)

def process_csv_files(input_dir, output_dir, max_workers=5):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(send_file_to_api, os.path.join(input_dir, file_name)): file_name for file_name in os.listdir(input_dir) if file_name.endswith('.csv')}
        
        for future in concurrent.futures.as_completed(future_to_file):
            response_data, full_file_path = future.result()
            file_name = os.path.basename(full_file_path)  # Extract just the file name
            save_json_response(response_data, output_dir, file_name)

def main():
    parser = argparse.ArgumentParser(description='Upload CSV files to TollGuru and get toll data.')
    parser.add_argument('--to_process', required=True, help='Path to the CSV folder.')
    parser.add_argument('--output_dir', required=True, help='Folder to store resulting JSON files.')

    args = parser.parse_args()
    process_csv_files(args.to_process,args.output_dir)

if __name__ == "__main__":
    main()