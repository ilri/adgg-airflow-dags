from airflow.providers.mysql.hooks.mysql import MySqlHook


class DatabaseManager:
    def __init__(self, connection_id):
        self.hook = MySqlHook(mysql_conn_id=connection_id)
        self.db_connection = self.hook.get_conn()

    def get_cursor(self):
        return self.db_connection.cursor()

    def execute_query(self, query):
        with self.get_cursor() as cursor:
            cursor.execute(query)
        self.db_connection.commit()

    def connect_to_database(self):
        with self.get_cursor() as cursor:
            cursor.execute("SET GLOBAL innodb_lock_wait_timeout=600;")

    def get_country_dict(self):
        query = "SELECT id , name as country_name FROM core_country"
        with self.get_cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        country_dict = {str(id): name for id, name in results}  # Convert id to string
        if not country_dict:
            raise Exception("Country dictionary is empty. Is the 'core_country' table populated?")
        return country_dict

    def fetch_data(self, query):
        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def close(self):
        self.db_connection.close()
