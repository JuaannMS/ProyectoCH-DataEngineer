
FROM apache/airflow:2.7.0

#Instalacion de dependencias
COPY requirements.txt /opt/airflow
RUN pip install -r /opt/airflow/requirements.txt

#Copia de Dag

COPY --chown=airflow:root . /opt/airflow/dags

RUN airflow db init
ENTRYPOINT airflow scheduler & airflow webserver
