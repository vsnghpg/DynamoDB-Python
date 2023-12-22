import csv
import json
import logging
import os
import subprocess

logging.basicConfig(level=logging.DEBUG)

def read_csv_chunked(csv_file_path, chunk_size=25):
    try:
        with open(csv_file_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader]

            for i in range(0, len(rows), chunk_size):
                yield rows[i:i + chunk_size]
    except FileNotFoundError:
        logging.error(f"File not found: {csv_file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        raise

def generate_dynamodb_json(data):
    dynamodb_data = []
    for item in data:
        dynamodb_item = {}
        for k, v in item.items():
            dynamodb_item[k] = {'N': v} if v.isdigit() else {'S': v}
        dynamodb_data.append({'PutRequest': {'Item': dynamodb_item}})
    return dynamodb_data

def write_json(json_file_path, tablename, dynamodb_json, chunk_number):
    try:
        base_filename, ext = os.path.splitext(json_file_path)
        chunked_json_file_path = f"{base_filename}_chunk{chunk_number}{ext}"

        with open(chunked_json_file_path, 'w', encoding='utf-8') as f:
            json_data = {tablename: dynamodb_json}
            json.dump(json_data, f, indent=4)
            logging.info(f"JSON written to {chunked_json_file_path}")
            return chunked_json_file_path
    except Exception as e:
        logging.error(f"Error writing JSON to file: {e}")
        raise

def execute_aws_command(json_file):
    # Replace the AWS CLI command with your actual command
    #aws_command = f"aws dynamodb put-item --table-name YourTableName --item file://{json_file}"
    aws_command = f"aws dynamodb batch-write-item --request-items file://{json_file}"
    
    try:
        subprocess.run(aws_command, shell=True, check=True)
        logging.info(f"AWS CLI command executed successfully for {json_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing AWS CLI command for {json_file}: {e}")
        raise

def process_csv_chunked(csv_file_path, json_file_path, tablename, chunk_size=25):
    try:
        generated_json_files = []
        for i, chunk in enumerate(read_csv_chunked(csv_file_path, chunk_size)):
            dynamodb_json = generate_dynamodb_json(chunk)
            json_file = write_json(json_file_path, tablename, dynamodb_json, i + 1)
            generated_json_files.append(json_file)
            execute_aws_command(json_file)

        logging.info("CSV processing completed successfully.")
        return generated_json_files
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        return None

if __name__ == "__main__":
    try:
        csv_file_path = input('Enter the absolute path of the CSV file: ').strip()
        json_file_path = input('Enter the base absolute path of the JSON files: ').strip()
        tablename = input('Enter the tablename: ').strip()

        # Basic input validation
        if not os.path.isfile(csv_file_path):
            raise ValueError(f"Invalid CSV file path: {csv_file_path}")

        if not os.path.isdir(os.path.dirname(json_file_path)):
            raise ValueError(f"Invalid JSON file directory: {os.path.dirname(json_file_path)}")

        generated_files = process_csv_chunked(csv_file_path, json_file_path, tablename)

        if generated_files:
            print("\nGenerated JSON files:")
            for file in generated_files:
                print(file)
    except KeyboardInterrupt:
        logging.info("Script execution interrupted by the user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
