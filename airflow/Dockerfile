FROM apache/airflow:2.5.1

# Cambiar al usuario 'airflow' para instalar dependencias
USER airflow

# Instalar dependencias de Airflow y paquetes adicionales
RUN pip install --no-cache-dir apache-airflow[postgres]==2.5.1

# Instalar dependencias adicionales (si es necesario)
RUN pip install --no-cache-dir requests pyspark python-dotenv

# Volver al usuario root para configuraciones adicionales (opcional)
USER root