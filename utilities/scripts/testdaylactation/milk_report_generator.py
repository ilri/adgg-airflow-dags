import json
import csv
import pandas as pd
import os
import zipfile
import dask.dataframe as dd
import concurrent.futures

from datetime import datetime
from time import sleep
from tqdm import tqdm
from .database_manager import DatabaseManager
from mysql.connector import errors
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from dask.distributed import Client
from collections import defaultdict


class MilkReportGenerator:
    def __init__(self, db_manager, country_name):
        self.db_manager = db_manager
        self.country_dict = self.db_manager.get_country_dict()
        self.country_name = country_name
        self.hook = MySqlHook(mysql_conn_id='adgg_production')
        self.cursor = self.db_manager.get_cursor()
        self.scripts_dir = f"{Variable.get('scripts_folder')}testdaylactation"
        self.output_dir = Variable.get("output_folder")
        self.default_email = Variable.get("default_email")
        self.reverse_country_dict = {v: k for k, v in self.country_dict.items()}
        if country_name not in self.reverse_country_dict:
            raise ValueError(f"Provided country name '{country_name}' does not exist in the country dictionary.")
        self.country_id = self.reverse_country_dict[country_name]

    def fetch_data(self, query):
        attempts = 0
        max_attempts = 5
        while attempts < max_attempts:
            try:
                return self.db_manager.fetch_data(query)
            except errors.OperationalError as err:
                # Lost connection to MySQL server during query
                if err.errno == 2013:
                    attempts += 1
                    # Wait a bit before retrying
                    sleep(5)
                else:
                    raise err
        raise Exception("Lost connection to MySQL server during query after several attempts")

    def lactation_query(self):
        return f"""
        SELECT
            animal_id AS animalid,
            DATE_FORMAT(calving_date, '%Y-%m-%d') AS calvdate,
            RANK() OVER (PARTITION BY animal_id ORDER BY calving_date) parity
        FROM (
            SELECT
            animal.tag_id AS tag_id,
            evnt.id AS lactation_id,
            animal.id AS animal_id,
            DATE_FORMAT(evnt.event_date, '%Y-%m-%d') AS calving_date,
            DATEDIFF(evnt.event_date, animal.birthdate) AS calving_age,
            LAG(evnt.event_date, 1) OVER (PARTITION BY evnt.animal_id ORDER BY evnt.event_date) AS previous_calving_date,
            DATEDIFF(evnt.event_date, LAG(evnt.event_date, 1) OVER (PARTITION BY evnt.animal_id ORDER BY evnt.event_date)) AS calving_interval
            FROM core_animal animal
            JOIN core_farm farm ON animal.farm_id = farm.id
            LEFT JOIN core_animal_event evnt ON animal.id = evnt.animal_id
            WHERE farm.country_id = {self.country_id} AND evnt.event_type = 1 
            ORDER BY animal.id) g
        WHERE (g.calving_interval IS NULL OR g.calving_interval >= 220) 
        """

    def testday_query(self):
        return f"""
        SELECT 
            evnt.animal_id animalid,
            DATE_FORMAT(evnt.event_date, '%Y-%m-%d') AS milkdate,
            anim.tag_id as tagid,
            anim.farm_id as farmid, 
            evnt.id as milkeventid
        FROM core_animal_event evnt 
        JOIN core_animal anim ON evnt.animal_id = anim.id
        JOIN core_farm farm ON anim.farm_id = farm.id
        WHERE (evnt.event_type = 2 and evnt.country_id = {self.country_id})
        GROUP BY anim.farm_id, evnt.animal_id, anim.tag_id, evnt.event_date, evnt.id
        HAVING COUNT(DISTINCT evnt.event_date) = 1 
        """

    @staticmethod
    def prepare_data(raw_data):
        # Separate the raw data into lactation and testday data
        lactation_data, testday_data = raw_data

        # Convert the lactation data into a dictionary for faster lookup
        lactation_dict = defaultdict(list)
        for row in lactation_data:
            animalid, calvdate, parity = row
            if calvdate:
                calvdate = datetime.strptime(calvdate, '%Y-%m-%d')
            lactation_dict[animalid].append((calvdate, parity))

        # Sort each animal's lactation data by calving date
        for lactations in lactation_dict.values():
            lactations.sort(key=lambda x: x[0] if x[0] else datetime.min)

        # Initialize the result list and testday counter
        data = []
        testday_count = defaultdict(int)

        # Process the testday data
        for row in tqdm(testday_data[1:], desc="Combining testday to lactation rows"):
            animalid, milkdate, tagid, farmid, milkeventid = row
            milkdate_obj = datetime.strptime(milkdate, '%Y-%m-%d')

            lactation_rows = lactation_dict[animalid]
            lactations_before_milkdate = [r for r in lactation_rows if r[0] is None or r[0] <= milkdate_obj]
            if lactations_before_milkdate:
                # Retrieve the most recent calving date and parity
                closest_calvdate, parity = lactations_before_milkdate[-1]
                # Update the testday count and retrieve the current testday number
                testday_count[animalid, parity] += 1
                testdaynumber = testday_count[animalid, parity]
            else:
                closest_calvdate, parity, testdaynumber = None, None, None

            data.append(
                {
                    'animalid': animalid,
                    'farmid': farmid,
                    'tagid': tagid,
                    'closest_calvdate': closest_calvdate,
                    'milkdate': milkdate,
                    'parity': parity,
                    'testdaynumber': testdaynumber,
                    'milkeventid': milkeventid
                }
            )
        report_testday_lacation_df = pd.DataFrame(data)
        return report_testday_lacation_df

    def fetch_and_merge_data(self, report_testday_lacation_df):
        final_report_query = f"""
                SELECT 
                    region.name as region, district.name as district, ward.name as ward, village.name as village,
                    core_animal.farm_id as Farm_id ,
                    gender.label as farmergender,
                    JSON_UNQUOTE(JSON_EXTRACT(core_farm .additional_attributes, '$."49"')) as cattletotalowned,
                    core_animal.tag_id,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."59"')) as MilkAM,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."61"')) as MilkPM,
                    (COALESCE(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."59"')), 0) + 
                    COALESCE(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."61"')), 0)) as TotalMilk,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."63"')) as MilkFat,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."64"')) as MilkProt,
                    core_animal.original_tag_id as original_tag_id,
                    core_farm.latitude as latitude, core_farm.longitude as longitude,
                    core_animal_event.id as event_id, core_animal.id as animal_id
                FROM core_animal_event
                    INNER JOIN core_animal on core_animal_event.animal_id = core_animal.id
                    INNER JOIN core_farm on core_animal.farm_id = core_farm.id
                    INNER JOIN country_units region ON core_animal.region_id = region.id
                    INNER JOIN country_units district ON core_animal.district_id = district.id
                    INNER JOIN country_units ward ON core_animal.ward_id = ward.id
                    INNER JOIN country_units village ON core_animal.village_id = village.id
                    LEFT JOIN core_master_list gender on core_farm.gender_code = gender.value and gender.list_type_id = 3
                WHERE core_animal.country_id = {self.country_id}
                """
        weight_query = f"""
                SELECT
                    animal_id,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."136"')) AS Weight,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."529"')) AS EstimatedWt,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."139"')) AS Bodyscore
                FROM core_animal_event where event_type = 6 AND core_animal_event.country_id = {self.country_id}
                """

        data = self.db_manager.fetch_data(final_report_query)
        weight_data = self.db_manager.fetch_data(weight_query)

        df_sql = pd.DataFrame(data, columns=['region', 'district', 'ward', 'village', 'Farm_id', 'farmergender', 'cattletotalowned',
                                             'tag_id', 'MilkAM', 'MilkPM', 'TotalMilk', 'MilkFat', 'MilkProt',
                                             'original_tag_id', 'latitude', 'longitude', 'event_id', 'animal_id'])
        weight_data_df = pd.DataFrame(weight_data, columns=['animal_id', 'Weight', 'EstimatedWt', 'Bodyscore'])
        print("data from sql dataframe", df_sql)

        # Now join df_sql with the report_testday_lacation_df
        df_sql['event_id'] = pd.to_numeric(df_sql['event_id'], errors='coerce')
        report_testday_lacation_df.rename(columns={'milkeventid': 'event_id'}, inplace=True)
        df_sql = report_testday_lacation_df.merge(df_sql, left_on='event_id', right_on='event_id', how='inner')
        df_sql = weight_data_df.merge(df_sql, left_on='animal_id', right_on='animal_id', how='right')

        # Filter data as needed
        df_sql['milkdate'] = pd.to_datetime(df_sql['milkdate'])
        df_sql['closest_calvdate'] = pd.to_datetime(df_sql['closest_calvdate'])
        df_sql['Days In Milk'] = (df_sql['milkdate'] - df_sql['closest_calvdate']).dt.days
        df_sql = df_sql[df_sql['Days In Milk'] >= 0]

        # Use the group by function in pandas to group by columns
        df_sql = df_sql.groupby(['animalid', 'milkdate', 'closest_calvdate']).first().reset_index()

        cols = ['region', 'district', 'ward', 'village', 'Farm_id', 'farmergender', 'cattletotalowned', 'tag_id',
                'closest_calvdate', 'milkdate', 'MilkAM', 'MilkPM', 'TotalMilk', 'Days In Milk', 'MilkFat',
                'MilkProt', 'Heartgirth', 'Weight', 'EstimatedWt', 'Bodyscore', 'parity',
                'testdaynumber', 'latitude', 'longitude', 'original_tag_id']

        df_sql = df_sql.reindex(columns=cols)

        return df_sql

    def write_to_csv(self, df_sql):
        output_dir = self.output_dir
        os.makedirs(output_dir, exist_ok=True)
        country_name = self.country_dict.get(self.country_id, "unknown_country")
        filename = f"{country_name}_testday_lactation_combined_output.csv"
        filename = os.path.join(output_dir, filename)

        df_sql.to_csv(filename, index=False, encoding='utf-8-sig')
        return filename

    @staticmethod
    def create_zip(filename):
        # Check if the file exists
        if not os.path.isfile(filename):
            print(f"Error: file '{filename}' not found. Unable to create zip file.")
            return None

        # If the file exists, create a zip file
        zipname = filename.replace('.csv', '.zip')
        with zipfile.ZipFile(zipname, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add file with just its basename (avoiding directory structure)
            zipf.write(filename, arcname=os.path.basename(filename))

        # Remove the original CSV file
        os.remove(filename)

        return zipname

    def main(self):
        # Fetch raw data
        lactation_data = self.fetch_data(self.lactation_query())
        testday_data = self.fetch_data(self.testday_query())

        # Prepare data
        processed_data = self.prepare_data((lactation_data, testday_data))

        # Generate final report
        result_data = self.fetch_and_merge_data(processed_data)

        # Write data to csv
        filename = self.write_to_csv(result_data)

        # Create a zip file from the CSV file
        zip_filename = self.create_zip(filename)

        # Provide the absolute path of the zip file.
        zip_file_path = os.path.abspath(zip_filename)
        # print(result_data.columns)

        return zip_file_path


if __name__ == '__main__':
    hook = MySqlHook(mysql_conn_id='adgg_production')
    scripts_dir = f"{Variable.get('scripts_folder')}testdaylactation"
    output_dir = Variable.get("output_folder")
    default_email = Variable.get("default_email")

    db_manager = DatabaseManager(connection_id=hook.conn_id)
    country_name = input("Please enter the country name: ")

    # report_gen = MilkReportGenerator(db_manager=db_manager, country_name=country_name)
    report_gen = MilkReportGenerator(db_manager=db_manager, country_name=country_name)
    report_gen.main()
