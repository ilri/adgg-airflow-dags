import os
import json
import csv
import requests
import zipfile
import mysql.connector
from dotenv import load_dotenv



load_dotenv()  # Load environment variables
current_file_path = os.path.abspath(__file__)
ENVIRONMENT = os.getenv("ENVIRONMENT")
BASE_FILE_PATH = os.getenv("BASE_FILE_PATH")
BASE_URL = os.getenv("BASE_URL")
SERVER_HEADERS = {'Authorization': f'token {os.getenv("ONA_TOKEN")}'}
BASE_FILE_PATH = os.path.dirname(
    os.path.dirname(current_file_path)) + '/dags/utilities/scripts/odk_media_files/media_files/'

if ENVIRONMENT == 'production':
    FORM_ID = os.getenv("FORM_ID")
    DB_PORT = os.getenv("DB_PORT")
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_NAME = os.getenv("DB_NAME")
else:
    FORM_ID = os.getenv("DEV_FORM_ID")
    DB_PORT = os.getenv("DEV_DB_PORT")
    DB_HOST = os.getenv("DEV_DB_HOST")
    DB_USER = os.getenv("DEV_DB_USER")
    DB_PASS = os.getenv("DEV_DB_PASS")
    DB_NAME = os.getenv("DEV_DB_NAME")

stored_procs = {
    "farmer_list": (1, DB_NAME),
    "animal_list": (2, DB_NAME),
    "farmer_list_extended": (3, DB_NAME),
    "lactation_file": (4, DB_NAME),
    "region_list": (5, DB_NAME),
    "zone_list": (6, DB_NAME),
    "ward_list": (7, DB_NAME),
    "village_list": (8, DB_NAME),
    "registered_ai_bulls_list": (9, DB_NAME),
    "form_controller": (10, DB_NAME),
    "staff_list": (11, DB_NAME),
    "staff_rights": (12, DB_NAME)
}

def execute_stored_procedure(option):
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )

        if connection.is_connected():
            cursor = connection.cursor()
            cursor.callproc('workdb.odk_media_files', option)

            results = None
            for result in cursor.stored_results():
                results = result.fetchall()

            cursor.close()
            connection.close()
            return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()

    return None


def write_to_csv(data, filename):
    base_file_path = BASE_FILE_PATH
    csv_file_path = os.path.join(base_file_path, filename)

    # Write data to CSV file
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

    # Create a zip file
    zip_file_path = os.path.join(base_file_path, os.path.splitext(filename)[0] + '.zip')
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(csv_file_path, os.path.basename(csv_file_path))

    # Remove the original CSV file
    os.remove(csv_file_path)

    # Return the path to the zip file
    return zip_file_path


def fetch_data_from_api(url, auth=None):
    response = requests.get(url=url, headers=SERVER_HEADERS, auth=auth)
    print(f"Response from {url}: {response.text}")
    return response


def check_response_status(response):
    if response.status_code == 200:
        return True
    else:
        print(f"Request to URL: {response.url} failed with status code {response.status_code}")
        return False


def parse_response_to_json(response):
    if response.text.strip() == "":
        print(f"Empty response from URL: {response.url}")
        return None
    try:
        print(f"Response content: {response.content}")  # print the content before parsing
        response_json = response.json()
        return response_json
    except json.JSONDecodeError:
        print(f"Unable to decode the response into JSON for URL: {response.url}")
        return None


def save_json_to_file(data, file_name):
    # Expanding the tilde
    file_name = os.path.expanduser(file_name)
    # Create directory if it does not exist
    directory = os.path.dirname(file_name)
    if not os.path.exists(directory):
        os.makedirs(directory)
    # Save the file
    with open(file_name, "w") as file:
        json.dump(data, file, indent=2)


def update_form_media(file_path, file_name):
    url = f"{BASE_URL}metadata?xform={FORM_ID}"
    FILE_PATH = file_path
    FILENAME = file_name
    MIME = 'text/csv'
    XFORM = FORM_ID

    response = requests.get(url, headers=SERVER_HEADERS)
    if response.status_code == 200:
        try:
            for each in response.json():
                if str(each['xform']) == XFORM and each['data_value'] == FILENAME:
                    media_id = each['id']
                    media_url = f"{BASE_URL}metadata/{media_id}"
                    requests.delete(url=media_url, headers=SERVER_HEADERS)
                    break

            with open(f'{FILE_PATH}{FILENAME}', 'rb') as file_data:
                files = {'data_file': (FILENAME, file_data.read(), MIME)}
                data = {
                    'data_value': FILENAME,
                    'xform': XFORM,
                    'data_type': 'media',
                    'data_file_type': MIME,
                }
                requests.post(url=url, data=data, files=files, headers=SERVER_HEADERS)
        except json.JSONDecodeError:
            print(f"Unable to decode the response into JSON for URL: {url}")
            print(f"Response content: {response.content}")
    else:
        print(f"Request to URL: {url} failed with status code {response.status_code}")


def main():

    for stored_proc, option in stored_procs.items():
        filename = f"{stored_proc}.csv"
        results = execute_stored_procedure(option)

        if results:
            write_to_csv(results, filename)
            print(f"Output saved to {filename}")
        else:
            print(f"No results for stored procedure: {stored_proc}")

    # Define the list of file names
    file_names = ['animal_list.zip', 'farmer_list.zip', 'farmer_list_extended.zip', 'lactation_file.zip',
                  'region_list.zip', 'zone_list.zip', 'ward_list.zip', 'village_list.zip', 'registered_ai_bulls_list.zip' , 'form_controller.zip','staff_list.zip','staff_rights.zip']
    # Update form media on the server for each file
    for file_name in file_names:
        update_form_media(file_path=BASE_FILE_PATH, file_name=file_name)


if __name__ == "__main__":
    main()
