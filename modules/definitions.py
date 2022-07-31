from datetime import datetime
import pandas as pd
import pandera as pa
import pandavro as pdx
import pymysql
import io


def save_logs_error(bucket, data_error, table):
    name = 'LOGS_' + table + datetime.now().strftime("%Y%m%d-%I%M%S%p")
    blob = bucket.blob('poc-ingest/logs/{}.csv'.format(name))
    blob.upload_from_string(
                data=data_error.to_csv(index=False),
                content_type='text/csv'
            )

def connect_database (db_user, db_pass, db_name, db_host):
    conection = pymysql.connect(
        host        = db_host,
        user        = db_user, 
        password    = db_pass,
        db          = db_name,
    )
    return conection

def check_table(conection, table_name):
    cursor = conection.cursor()
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

def return_tables(conection):
    cursor = conection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables a WHERE TABLE_SCHEMA  = 'mydbpoc'")
    tablas = cursor.fetchall()
    cursor.close()
    ls = []
    for td in list(tablas): 
        ls.append(list(td)[0])
    return ls  

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

def clean_create_table (conection, table_name, columns_table, columns_types):
    cursor = conection.cursor()
    if check_table(conection, table_name):
        query = ("TRUNCATE TABLE {0}" ).format(table_name)    
    else:
        #Cambia el tipo de dato para lo 'str'  
        #ilist = columns_types.index('str')
        #columns_types = columns_types[:ilist]+['varchar(100)']+columns_types[ilist+1:]
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
    print(query)
    cursor.execute(query)
    conection.commit()
    cursor.close()  

def query_batch(table_name, columns_query):
    strc, val = str_columns(columns_query)
    query = ("INSERT INTO {0} {1} VALUES {2}").format(table_name, strc, val)
    return query

def load_data(conection, table_name, columns_query, data, batch_size=10000):
    query = query_batch(table_name, columns_query)
    cursor = conection.cursor()
    print(query)
    for i in range(0, len(data), batch_size):
        cursor.executemany(query, data[i:i+batch_size])
        conection.commit()
        print('Datos cargados...')
    cursor.close()

def ingest_data(conection, bucket, filename, table_name, columns_table, schema_table, types_table,  chunksize = 1000):
    print('Se inicia analisis datos...')
    #Variables 
    data_validate = pd.DataFrame(columns=columns_table)
    data_error = pd.DataFrame(columns=columns_table)
    indexs_errors = pd.Series([],dtype=pd.Int32Dtype()) 
    #Data Blob
    blob = bucket.blob(filename, chunk_size = 256 * 1024)
    data = blob.download_as_string()
   
    for chunk in pd.read_csv(io.BytesIO(data), names=columns_table, header=None, chunksize=chunksize):
        try:
            schema_table(chunk, lazy=True)
        except pa.errors.SchemaErrors as exc:
            indexs_errors = pd.concat([indexs_errors, exc.failure_cases["index"]])
            print('Cantidad de errores: ', len(indexs_errors.index))
        data_validate = pd.concat([data_validate, chunk[~chunk.index.isin(indexs_errors)]])
        data_error = pd.concat([data_error, chunk[chunk.index.isin(indexs_errors)]])
    print('Se valida tabla: ' + table_name)
    clean_create_table (conection, table_name, columns_table, types_table)
    print('Se ingesta datos...')
    load_data(conection, table_name, columns_table, data_validate.values.tolist(), batch_size=1000)

    if len(data_error) > 0:
        save_logs_error(bucket, data_error, table_name)

def backup_avro(conection):
    tables = return_tables(conection)
    for table in tables:
        df_table = pd.DataFrame()
        table_name = table
        query = ("SELECT * FROM {}").format(table_name)
        df_table = pd.read_sql(query, con = conection)
        pdx.to_avro('./files_avro/{}.avro'.format(table_name), df_table)

def rest_avro_table(engine, table_name):
    engine.execute('TRUNCATE TABLE {}'.format(table_name))
    data = pdx.read_avro('./files_avro/{}.avro'.format(table_name))
    data.to_sql(table_name, engine, if_exists='append',index=False)