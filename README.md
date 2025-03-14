# Configuracion del Proyecto

## 1. Clonar el Repositorio
```bash
git clone https://github.com/tu-usuario/tu-repositorio.git
cd INSTRUMENTOS_DWH_POC
```
## 2. Configurar Variables de Entorno
Crea un archivo .env en la raíz del proyecto con las siguientes variables:
```
API_KEY=YDMGB3KH33ZX10HA
API_URL=https://www.alphavantage.co
HOST=localhost
DBNAME=INSTRUMENTOS_DWH
USER=admin
PASSWORD=admin
DBPORT=5432
```

# Configuración de los Servicios

## 1. Configurar Apache Airflow
### Acceder a Airflow:
#### en el navegador
```
http://localhost:8081
```
#### inicia sesion
```
airflow/airflow
```

#### activa el DAG
```
pipeline_completo
```

## 2. Configurar Apache Superset
### Acceder a Superset
#### en el navegador
```
http://localhost:8082
```
#### inicia sesion
```
admin/admin
```


### FICHERO DE PROYECTO
```
│   .env
│   .gitignore
│   docker-compose.yml
│   requirements.txt
│
├───airflow
│   │   airflow.cfg
│   │   Dockerfile
│   │   requirements.txt
│   │
│   └───dags
│           pipeline.py
│
├───carga
│   │   database_operation.py
│   │   dwh_operation.py
│   │   staging_operation.py
│   │   requirements.txt
│   │
│   └───__pycache__
│
├───extraccion
│   │   extraccion_api.py
│   │   extraccion_data.py
│   │   requirements.txt
│   │
│   └───__pycache__
│
├───scripts
│   │   db_creacion.sql
│   │   resumen_script.sql
│
└───superset
    │   Dockerfile
    │   superset_init.sh
```
