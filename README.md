# API Rest migrar CSV a SQL 

Es una API Rest para ingestar datos en formato CSV desde Storage a Cloud SQL MySQL.

## Detalle de API Rest

* `/ingestar_datos/<string:table>` migra todos las tablas desde Storage hacia Cloud SQL MySQL.
* `/backup_tablas` realiza un backup de todas las tablas en formato AVRO en fichero.
* `/restaurar_tablas/<string:table>` restaura una tabla especifica desde el backup de tablas AVRO.
* `/consultar_datos/<string:table>` consultar datos de tabla de Cloud SQL MySQL.

## Pruebas

Para realizar pruebas se ha despleado en Cloud RUN
[API Rest PoC](https://poc-ingest-dev-z22wuxtb7a-uc.a.run.app/)

### Testing tables
* departments
* jobs
* hired_employees
* all

Ejemplos: 
* Para ingestar todas las tablas ../ingestar_datos/all
* Para ingestar una tabla  ../ingestar_datos/jobs

## Data Studio para explorar datos

[Report Data Studio End-Points](https://datastudio.google.com/reporting/fafe96ab-ae15-4f6b-be6b-722d90c58bb2/page/Fr5yC)

## Implementaciones

### Instrucciones para desplegar desde Local

1. Asegurarse que estes intalados todas las librerias de python detallados en el archivo requirements.txt.
2. Ejecutar main.py

### Instrucciones para desplegar imagen desde VS Code

1. Instalar la extensión `Cloud Code` desde la libreria.
2. Iniciar sesión cuenta GCP desde VS Code.
3. Seleccionar proyecto.
4. Desde la barra de estado Cloud Code elegir `Deploy to Cloud Run`.
5. Desplegar.

## Requerimientos

* Docker
* Python > 3.9