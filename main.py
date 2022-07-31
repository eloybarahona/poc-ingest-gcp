from flask import Flask
from flask import jsonify
from modules.definitions import *
from modules.dictionary import *
from config import *
import os

app = Flask(__name__)

@app.route('/')
def index():
    return "Testing API Rest"
  
@app.route('/ingestar_datos/<string:table>', methods=('GET', 'POST'))
def ingest_datos(table):
    try:
        conection = mycon()
        storage_client, bucket_name = myclient()
        bucket = storage_client.bucket(bucket_name)
        blobs = storage_client.list_blobs(bucket_name)
        for blob in blobs:
            if 'poc-ingest/' in blob.name  and '.csv' in blob.name and 'logs' not in  blob.name:
                #print('Blob: ', blob.name)
                filename = blob.name
                table_name = filename.replace('poc-ingest/','').replace('.csv','')
                if table == 'all' or table == table_name:
                    columns_table = globals()['columns_' + table_name]
                    types_table = globals()['types_' + table_name]
                    schema_table = globals()['schema_' + table_name]
                    ingest_data(conection, bucket, filename, table_name, columns_table, schema_table, types_table)
        return jsonify({'Mensaje': "Ok", 'Resultado': True, 'Descripcion' : 'Tabla ' + table})
    except Exception as error:
        print('Error :' + str(error))
        return jsonify({'Mensaje': "Error", 'Resultado': False, 'Descripcion' : str(error)})

@app.route('/backup_tablas', methods=('GET', 'POST'))
def backup_tables():
    try:
        conection = mycon()
        backup_avro(conection)
        return jsonify({'Mensaje': "Ok", 'Resultado': True, 'Descripcion' : 'Se realizo el backup de las tablas en AVRO'})
    except Exception as error:
        print('Error :' + str(error))
        return jsonify({'Mensaje': "Error", 'Resultado': False, 'Descripcion' : str(error)})

@app.route('/restaurar_tablas/<string:table>', methods=('GET', 'POST'))
def restaurar_tablas(table):
    try:
        engine = myengine()
        rest_avro_table(engine, table)
        return jsonify({'Mensaje': "Ok", 'Resultado': True, 'Descripcion' : 'Se restauro la tabla ' + table})
    except Exception as error:
        print('Error :' + str(error))
        return jsonify({'Mensaje': "Error", 'Resultado': False, 'Descripcion' : 'Tabla no existe'})

@app.route('/consultar_datos/<string:table>', methods=('GET', 'POST'))
def consultar_datos(table):
    try:
        conection = mycon()
        cursor = conection.cursor()
        columns_table = globals()['columns_' + table]
        col, val = str_columns(columns_table)
        col = col.replace('(','').replace(')','')
        sql = ("SELECT {0} FROM {1}").format(col, table)
        cursor.execute(sql)
        datos = cursor.fetchall()
        filas = []
        for fila in datos:
            filas.append(fila)
        json_string = json.dumps(filas)
        conection.close()
        return jsonify({'Datos': json_string, 'mensaje': "Datos listados.", 'exito': True})
    except Exception as error:
        print('Error :' + str(error))
        conection.close()
        return jsonify({'Mensaje': "Error", 'Resultado': False, 'Descripcion' : str(error)})

if __name__ == '__main__':
    app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True)
