from pandera import Column
import pandera as pa

columns_hired_employees = ['id', 'name', 'datetime', 'departament_id', 'job_id']
columns_jobs = ['id', 'job']
columns_departments = ['id', 'department']

types_hired_employees = ['int', 'varchar(100)', 'varchar(100)', 'int', 'int']
types_jobs = ['int', 'varchar(100)']
types_departments = ['int', 'varchar(100)']

schema_hired_employees = pa.DataFrameSchema(
    {
        "id": Column(int),
        "name": Column(str),
        'datetime': Column(str),
        'departament_id': Column(int),
        'job_id': Column(int)
    }
)

schema_jobs = pa.DataFrameSchema(
    {
        "id": Column(int),
        "job": Column(str)
    }
)

schema_departments = pa.DataFrameSchema(
    {
        "id": Column(int),
        "department": Column(str)
    }
)