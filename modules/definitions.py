import pandas as pd
import pandera as pa
import pymysql
import io

def validate_dataframe(bucket, filename, columns_names, schema_table, chunksize = 1000):
    #Variables 
    data_validate = pd.DataFrame(columns=columns_names)
    data_error = pd.DataFrame(columns=columns_names)
    indexs_errors = pd.Series([],dtype=pd.Int32Dtype()) 

    #Data Blob
    blob = bucket.blob(filename, chunk_size = 256 * 1024)
    data = blob.download_as_string()
   
    for chunk in pd.read_csv(io.BytesIO(data), names=columns_names, header=None, chunksize=chunksize):
        try:
            schema_table(chunk, lazy=True)
        except pa.errors.SchemaErrors as exc:
            indexs_errors = pd.concat([indexs_errors, exc.failure_cases["index"]])
            print('Cantidad de errores: ', len(indexs_errors.index))
        data_validate = pd.concat([data_validate, chunk[~chunk.index.isin(indexs_errors)]])
        data_error = pd.concat([data_error, chunk[chunk.index.isin(indexs_errors)]])

    return data_validate, data_error

def connect_database (db_user, db_pass, db_name, db_host):
    connection = pymysql.connect(
        host        = db_host,
        user        = db_user, 
        password    = db_pass,
        db          = db_name,
    )
    cursor = connection.cursor()
    return connection, cursor

def str_columns(columns_query):
    lc = len(columns_query)
    strc = ''
    val = ''
    for index, column in enumerate(columns_query):
        if index == 0:
            strc = strc + '(' + column  + ', '  
            val = val + '(%s, ' 
        elif index < lc - 1:
            strc = strc + column + ', ' 
            val = val + '%s, '
        elif index == lc - 1:
            strc = strc + column + ')'
            val = val + '%s)'
    return strc, val

def check_table(connection, table_name):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(table_name.replace('\'', '\'\'')))
    if cursor.fetchone()[0] == 1:
        cursor.close()
        return True
    cursor.close()
    return False

def clean_create_table (connection, table_name, columns_table, columns_types):
    cursor = connection.cursor()
    if check_table(connection, table_name):
        query = ("TRUNCATE TABLE {0}" ).format(table_name)    
    else:
        #Cambia el tipo de dato para lo 'str'  
        ilist = columns_types.index('str')
        columns_types = columns_types[:ilist]+['varchar(100)']+columns_types[ilist+1:]
        lc = len(columns_table)
        strc = ''
        val = ''
        for index, column in enumerate(columns_table):
            if index == 0:
                strc = strc + '(' + column  + ' ' + columns_types[index] + ', '
            elif index < lc - 1:
                strc = strc + column + ' ' + columns_types[index] + ', '
            elif index == lc - 1:
                strc = strc + column + ' ' + columns_types[index] + ')'
        query = ("CREATE TABLE {0} {1}" ).format(table_name, strc)
    cursor.execute(query)
    connection.commit()
    cursor.close()  

def query_batch(strc, val):
    query = ("INSERT INTO mytable {0} VALUES {1}").format(strc, val)
    return query

def load_orders(connection, query, data, batch_size=10000):
    cursor = connection.cursor()
    for i in range(0, len(data), batch_size):
        cursor.executemany(query, data[i:i+batch_size])
        connection.commit()
        print('Datos cargados...')
    cursor.close()