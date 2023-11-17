from azure.identity import DefaultAzureCredential
from flask import Flask
import pyodbc, struct

server_name = 'ksagro-db-server.database.windows.net'
database_name = 'normalyze-test_2023-11-11T16-56Z'
db_driver = 'ODBC Driver 17 for SQL Server'
authentication = 'ActiveDirectoryMsi'

# Set up the connection string
connection_string = f"DRIVER={db_driver}; SERVER={server_name}; DATABASE={database_name};"
app = Flask(__name__)

@app.get('/')
def root_path():
    return "This is the root of the application"


@app.get('/get-persons')
def get_persons():
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("select * from Persons")
            rows = cursor.fetchall()
            op = ""
            for row in rows:
                op += f'<h2>{format_row(row)}</h2>'
        return op
    except Exception as e:
        return str(e)
    
@app.get('/get-person/id/<person_id>')
def get_person_id(person_id: int):
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"select * from Persons where PersonID = {person_id}")
            rows = cursor.fetchall()
            op = ""
            for row in rows:
                op += f'<h2>{format_row(row)}</h2>'
        op = f"The person with person id {person_id} does not exist" if op == "" else op
        return op
    except Exception as e:
        return str(e)


@app.get('/get-person/first-name/<first_name>')
def get_person_first_name(first_name: str):
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"select * from Persons where FirstName = '{first_name}'")
            rows = cursor.fetchall()
            op = ""
            for row in rows:
                op += f'<h2>{format_row(row)}</h2>'
        op = f"The person with First Name {first_name} does not exist" if op == "" else op
        return op
    except Exception as e:
        return str(e)


@app.get('/get-person/last-name/<last_name>')
def get_person_last_name(last_name: str):
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"select * from Persons where LastName = '{last_name}'")
            rows = cursor.fetchall()
            op = ""
            for row in rows:
                op += f'<h2>{format_row(row)}</h2>'
        op = f"The person with Last Name {last_name} does not exist" if op == "" else op
        return op
    except Exception as e:
        return str(e)


def get_connection():
    client_id = "9586f16a-6d9c-4e9a-b5e8-127490b5a8cd"
    # user_object_id = "1ac4a2ad-4609-43e0-bd21-3a42736740fc"
    tokenProvider = DefaultAzureCredential(managed_identity_client_id=client_id)
    resource = "https://database.windows.net/.default"
    access_token = tokenProvider.get_token(resource).token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(access_token)}s', len(access_token), access_token)

    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn



def format_row(row):
    return f'Id - {row.PersonID}, FirstName - {row.FirstName}, LastName - {row.LastName}'


if __name__ == '__main__':
    app.run(debug=True)