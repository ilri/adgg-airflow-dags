import pandas as pd
import os

from datetime import datetime
from time import sleep
from tqdm import tqdm
from collections import defaultdict
from .database_manager import DatabaseManager
from mysql.connector import errors


# new
class MilkReportGenerator:
    def __init__(self, db_manager, country_name, scripts_dir, output_dir):
        self.db_manager = db_manager
        self.scripts_dir = scripts_dir
        self.output_dir = output_dir
        self.country_name = country_name

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
            WHERE farm.country_id = {self.country_name} AND evnt.event_type = 1 
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
        WHERE (evnt.event_type = 2 and evnt.country_id = {self.country_name})
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
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."68"')) as MilkMidDay,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."61"')) as MilkPM,
                    (COALESCE(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."59"')), 0) + 
                    COALESCE(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."68"')), 0) + 
                    COALESCE(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."61"')), 0)) as TotalMilk,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."63"')) as MilkFat,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."64"')) as MilkProt,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."66"')) as SCC,
                    core_animal.original_tag_id as original_tag_id,
                    core_farm.latitude as latitude, core_farm.longitude as longitude,
                    core_animal_event.id as event_id, core_animal.id as animal_id,
                    core_farm.name as farmer_name, core_farm.id as farm_id, project, birthdate,farmtype.label as farmtype
                FROM core_animal_event
                    INNER JOIN core_animal on core_animal_event.animal_id = core_animal.id
                    INNER JOIN core_farm on core_animal.farm_id = core_farm.id
                    LEFT JOIN country_units region ON core_animal.region_id = region.id
                    LEFT JOIN country_units district ON core_animal.district_id = district.id
                    LEFT JOIN country_units ward ON core_animal.ward_id = ward.id
                    LEFT JOIN country_units village ON core_animal.village_id = village.id
                    LEFT JOIN core_master_list gender on core_farm.gender_code = gender.value and gender.list_type_id = 3
                    LEFT JOIN core_master_list farmtype on core_farm.farm_type = farmtype.value and farmtype.list_type_id = 2
                WHERE core_animal.country_id = {self.country_name}
                """
        weight_query = f"""
                SELECT
                    animal_id,
                    core_animal_event.event_date as weightdate,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."136"')) AS Weight,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."529"')) AS EstimatedWt,
                    JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."139"')) AS Bodyscore
                FROM core_animal_event where event_type = 6 AND core_animal_event.country_id = {self.country_name}
                """
        data = self.db_manager.fetch_data(final_report_query)
        weight_data = self.db_manager.fetch_data(weight_query)
        df_sql = pd.DataFrame(data, columns=['region', 'district', 'ward', 'village', 'Farm_id', 'farmergender',
                                             'cattletotalowned',
                                             'tag_id', 'MilkAM', 'MilkMidDay', 'MilkPM', 'TotalMilk', 'MilkFat', 'MilkProt','SCC',
                                             'original_tag_id', 'latitude', 'longitude', 'event_id', 'animal_id', 'farmer_name', 'farm_id', 'project',  'birthdate', 'farmtype'])
        weight_data_df = pd.DataFrame(weight_data, columns=['animal_id', 'weightdate', 'Weight', 'EstimatedWt', 'Bodyscore'])
        # Now join df_sql with the report_testday_lacation_df
        df_sql['event_id'] = pd.to_numeric(df_sql['event_id'], errors='coerce')
        report_testday_lacation_df.rename(columns={'milkeventid': 'event_id'}, inplace=True)
        df_sql = report_testday_lacation_df.merge(df_sql, left_on='event_id', right_on='event_id', how='inner')
        # df_sql = weight_data_df.merge(df_sql, left_on='animal_id', right_on='animal_id', how='right')
        df_sql = weight_data_df.merge(df_sql, on=['animal_id', 'weightdate', 'milkdate'], how='right')
        # Filter data as needed
        df_sql['milkdate'] = pd.to_datetime(df_sql['milkdate'], errors='coerce')
        df_sql['closest_calvdate'] = pd.to_datetime(df_sql['closest_calvdate'])
        df_sql['Days In Milk'] = (df_sql['milkdate'] - df_sql['closest_calvdate']).dt.days
        df_sql = df_sql[df_sql['Days In Milk'] >= 0]
        # Use the group by function in pandas to group by columns
        df_sql = df_sql.groupby(['animalid', 'milkdate', 'closest_calvdate']).first().reset_index()
        cols = ['region', 'district', 'ward', 'village', 'Farm_id', 'farmergender', 'cattletotalowned', 'tag_id', 'animalid',
                'closest_calvdate', 'milkdate',  'MilkAM', 'MilkMidDay', 'MilkPM', 'TotalMilk', 'Days In Milk', 'MilkFat',
                'MilkProt','SCC', 'Heartgirth', 'Weight', 'EstimatedWt', 'Bodyscore', 'parity',
                'testdaynumber', 'latitude', 'longitude', 'original_tag_id', 'event_id', 'farmer_name', 'farm_id', 'project', 'birthdate', 'farmtype']
        df_sql = df_sql.reindex(columns=cols)
        return df_sql

    def write_to_csv(self, df_sql):
        filename = f"testday_lactation_combined_output.csv"
        filename = os.path.join(self.output_dir, filename)
        df_sql.to_csv(filename, index=False, encoding='utf-8-sig')
        return filename

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

        return filename


if __name__ == '__main__':
    db_manager = DatabaseManager(connection_id="mysql_adgg_db_production")

    # Get the directory of the current DAG file
    dag_folder = os.path.dirname(os.path.abspath(__file__))

    # Define the paths for scripts and output directories
    scripts_dir = os.path.join(dag_folder, 'utilities', 'scripts', 'testdaylactation')
    output_dir = os.path.join(dag_folder, 'utilities', 'output')

    country_name = input("Please enter the country name: ")
    report_gen = MilkReportGenerator(database_manager=db_manager, country_name=country_name, scripts_dir=scripts_dir,
                                     output_dir=output_dir)
    report_gen.main()
