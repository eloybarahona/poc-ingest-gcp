from pandera import Column
import pandera as pa

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