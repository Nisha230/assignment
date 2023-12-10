import os
import json
import csv
import argparse

def extract_data_from_json(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
        tolls = data.get('route', {}).get('tolls', [])
        
        extracted_data = []
        for toll_entry in tolls:
            extracted_data.append({
                'unit': data.get('meta', {}).get('userId', ''),
                'trip_id': os.path.splitext(os.path.basename(json_file))[0],
                'toll_loc_id_start': toll_entry.get('start', {}).get('id', ''),
                'toll_loc_id_end': toll_entry.get('end', {}).get('id', ''),
                'toll_loc_name_start': toll_entry.get('start', {}).get('name', ''),
                'toll_loc_name_end': toll_entry.get('end', {}).get('name', ''),
                'toll_system_type': toll_entry.get('type', ''),
                'entry_time': toll_entry.get('start', {}).get('timestamp_formatted', ''),
                'exit_time': toll_entry.get('end', {}).get('timestamp_formatted', ''),
                'tag_cost': toll_entry.get('tagCost', ''),
                'cash_cost': toll_entry.get('cashCost', ''),
                'license_plate_cost': toll_entry.get('licensePlateCost', '')
            })
        return extracted_data

def process_json_files(input_dir, output_dir):
    headers = [
        'unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end', 
        'toll_loc_name_start', 'toll_loc_name_end', 'toll_system_type', 
        'entry_time', 'exit_time', 'tag_cost', 'cash_cost', 'license_plate_cost'
    ]

    output_csv = os.path.join(output_dir, 'transformed_data.csv')
    os.makedirs(output_dir, exist_ok=True)

    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()

        for file_name in os.listdir(input_dir):
            if file_name.endswith('.json'):
                file_path = os.path.join(input_dir, file_name)
                extracted_data = extract_data_from_json(file_path)
                for data in extracted_data:
                    writer.writerow(data)

def main():
    parser = argparse.ArgumentParser(description='Process JSON files and create a consolidated CSV file.')
    parser.add_argument('--to_process', required=True, help='Path to the JSON files directory.')
    parser.add_argument('--output_dir', required=True, help='Path to the output directory where transformed_data.csv will be stored.')

    args = parser.parse_args()
    process_json_files(args.to_process, args.output_dir)

if __name__ == "__main__":
    main()