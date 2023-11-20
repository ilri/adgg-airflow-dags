import os
from utilities.scripts.testdaylactation.database_manager import DatabaseManager
import json


class GpsUpdate:
    @staticmethod
    def update_gps_coordinates():
        # Get the directory of the current DAG file
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        # Define the paths for scripts and output directories
        scripts_dir = os.path.join(dag_folder, 'utilities', 'scripts', 'gps')

        try:
            # Database connection details
            db_manager = DatabaseManager(connection_id="mysql_adgg_db_production")

            # Connect to the database
            db_manager.connect_to_database()

            # Fetch the necessary data using DatabaseManager's fetch_data method
            query_fetch_odk_form = """
            SELECT form_data, form_uuid
            FROM adgg_uat.core_odk_form
            WHERE form_version LIKE "Ver 1.7" 
                AND farm_data IS NOT NULL
                # AND created_at >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH);
            """
            results = db_manager.fetch_data(query_fetch_odk_form)

            # Update GPS coordinates in adgg.core_farm for matching form_uuid
            for result in results:
                form_data_json = json.loads(result[0])

                # Flag to track if GPS location is found
                gps_location_found = False
                latitude, longitude = None, None

                if 'farmer_general' in form_data_json:
                    farmer_general_data = form_data_json['farmer_general']

                    if isinstance(farmer_general_data, list) and len(farmer_general_data) > 0:
                        for farmer_data in farmer_general_data:
                            for key, value in farmer_data.items():
                                if key.lower().endswith('farmer_gpslocation') and value is not None:
                                    coordinates = value.split(' ')
                                    if len(coordinates) >= 2:
                                        latitude = coordinates[0]
                                        longitude = coordinates[1]
                                        gps_location_found = True
                                        break

                            if gps_location_found:
                                break

                    # Update latitude and longitude in adgg.core_farm for matching form_uuid
                    if gps_location_found:
                        query_update_farm = f"""
                        UPDATE adgg_uat.core_farm 
                        SET latitude = '{latitude}', longitude = '{longitude}'
                        WHERE odk_form_uuid = '{result[1]}'
                        """
                        db_manager.execute_query(query_update_farm)

            print("Latitude and Longitude updated successfully.")

        except Exception as e:
            print(f"Error occurred: {str(e)}")

        finally:
            # Close the connection
            db_manager.close()

# If you want to test the function independently, you can call it directly
# GpsUpdate.update_gps_coordinates()
