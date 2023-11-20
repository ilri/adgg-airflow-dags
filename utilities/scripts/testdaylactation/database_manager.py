from airflow.providers.mysql.hooks.mysql import MySqlHook


# new
class DatabaseManager:
    def __init__(self, connection_id):
        self.hook = MySqlHook(mysql_conn_id=connection_id)
        self.db_connection = self.hook.get_conn()

    def get_cursor(self):
        return self.db_connection.cursor()

    def connect_to_database(self):
        with self.get_cursor() as cursor:
            cursor.execute("SET GLOBAL innodb_lock_wait_timeout=600;")

    def fetch_data(self, query):
        with self.get_cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def execute_query(self, query):
        with self.get_cursor() as cursor:
            cursor.execute(query)
            self.db_connection.commit()

    def execute_query(self, query):
        with self.get_cursor() as cursor:
            cursor.execute(query)
            self.db_connection.commit()

    def close(self):
        self.db_connection.close()
