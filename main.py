import fdb
import psycopg2

class DatabaseMigrator:
    def __init__(self, firebird_conn_params, postgres_conn_params):
        self.firebird_conn_params = firebird_conn_params
        self.postgres_conn_params = postgres_conn_params
        self.firebird_conn = None
        self.postgres_conn = None
        self.firebird_cursor = None
        self.postgres_cursor = None

    def connect_firebird(self):
        self.firebird_conn = fdb.connect(**self.firebird_conn_params)
        self.firebird_cursor = self.firebird_conn.cursor()

    def connect_postgres(self):
        self.postgres_conn = psycopg2.connect(**self.postgres_conn_params)
        self.postgres_cursor = self.postgres_conn.cursor()

    def create_table_in_postgres(self, table_name, column_defs):
        create_table_query = f'CREATE TABLE {table_name} ({", ".join(column_defs)})'
        self.postgres_cursor.execute(create_table_query)

    def fetch_firebird_data(self, table_name):
        self.firebird_cursor.execute(f'SELECT * FROM {table_name}')
        return self.firebird_cursor.fetchall()

    def insert_data_into_postgres(self, table_name, data):
        for row in data:
            values = ', '.join(map(repr, row))
            self.postgres_cursor.execute(f'INSERT INTO {table_name} VALUES ({values})')

    def execute_sql(self, sql, target='postgres'):
        if target == 'postgres':
            self.postgres_cursor.execute(sql)
        elif target == 'firebird':
            self.firebird_cursor.execute(sql)
        else:
            raise ValueError("Invalid target database. Use 'postgres' or 'firebird'.")

    def commit_postgres(self):
        self.postgres_conn.commit()

    def close_connections(self):
        if self.firebird_conn:
            self.firebird_conn.close()
        if self.postgres_conn:
            self.postgres_conn.close()

if __name__ == "__main__":
    # Пример использования класса для миграции структуры таблиц
    firebird_conn_params = {
        'dsn': '',
        'user': '',
        'password': '',
        'charset': 'UTF8'
    }

    postgres_conn_params = {
        'dbname': '',
        'user': '',
        'password': '',
        'host': 'localhost',
        'port': 5432
    }

    migrator = DatabaseMigrator(firebird_conn_params, postgres_conn_params)

    # Подключение к Firebird
    migrator.connect_firebird()

    # Подключение к PostgreSQL
    migrator.connect_postgres()

    # Список таблиц, которые вы хотите скопировать
    tables_to_copy = ['', '']

    # Перебираем таблицы и создаем их копии в PostgreSQL и Firebird
    for table_name in tables_to_copy:
        migrator.execute_sql(f'SELECT * FROM {table_name}', target='firebird')
        columns = migrator.firebird_cursor.description
        column_defs = [f'{col[0]} {col[1]}' for col in columns]

        # Создаем таблицу в PostgreSQL
        migrator.create_table_in_postgres(table_name, column_defs)

        # Выбираем данные из Firebird
        data = migrator.fetch_firebird_data(table_name)

        # Вставляем данные в PostgreSQL
        migrator.insert_data_into_postgres(table_name, data)

    # Фиксируем изменения и закрываем соединения
    migrator.commit_postgres()
    migrator.close_connections()

    print("Таблицы успешно созданы.")
