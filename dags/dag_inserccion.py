from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import requests
import csv
from sqlalchemy import create_engine
import pandas as pd
import smtplib
from db_config import db_params, credential_gmail

default_args = {
    'owner': 'Juan',
    'start_date': datetime(2023, 9, 26),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'etl_workflow',
    default_args=default_args,
    description='ETL',
    schedule_interval=timedelta(hours=1),
)


def fetch_data():
    api_url = "https://mindicador.cl/api"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(
            f"Failed to fetch data from the API. Status code: {response.status_code}")


fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)


def write_to_csv(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    indicadores = {
        "uf": data["uf"],
        "ivp": data["ivp"],
        "dolar": data["dolar"],
        "dolar_intercambio": data["dolar_intercambio"],
        "euro": data["euro"],
        "ipc": data["ipc"],
        "utm": data["utm"],
        "imacec": data["imacec"],
        "tpm": data["tpm"],
        "libra_cobre": data["libra_cobre"],
        "tasa_desempleo": data["tasa_desempleo"],
        "bitcoin": data["bitcoin"]
    }

    csv_filename = "indicadores.csv"
    csv_columns = ["codigo", "nombre",
                   "unidad_medida", "fecha_api", "valor", "fecha_ingreso_bd"]

    with open(csv_filename, "w", newline="", encoding="utf-8") as csv_file:

        csv_writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
        csv_writer.writeheader()

        fecha_ingreso_bd = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for indicador in indicadores.values():

            indicador['fecha_api'] = indicador.pop('fecha')
            indicador['fecha_ingreso_bd'] = fecha_ingreso_bd

            csv_writer.writerow(indicador)


write_to_csv_task = PythonOperator(
    task_id='write_to_csv',
    python_callable=write_to_csv,
    provide_context=True,
    dag=dag,
)


def read_csv():
    csv_filename = "indicadores.csv"
    df = pd.read_csv(csv_filename)
    return df


read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag,
)


def get_db_connection_info():
    return {
        'db_params': db_params,
        'credential_gmail': credential_gmail
    }


get_db_connection_info_task = PythonOperator(
    task_id='get_db_connection_info',
    python_callable=get_db_connection_info,
    dag=dag,
)


def insert_update_db(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='read_csv')
    db_params = ti.xcom_pull(task_ids='get_db_connection_info')['db_params']

    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"

    engine = create_engine(db_url)

    connection = engine.connect()

    for _, row in df.iterrows():
        table_name = row["codigo"]

        # Verifica si la fecha ya existe en la tabla antes de insertar

        df_to_insert = pd.DataFrame(
            data=[row], columns=['codigo', 'nombre', 'unidad_medida', 'fecha_api', 'valor', 'fecha_ingreso_bd'])
        df_to_insert.to_sql(
            table_name, connection, if_exists='append', index=False)
        print(f"Datos insertados en la tabla {table_name}")

    engine.dispose()


insert_update_db_task = PythonOperator(
    task_id='insert_update_db',
    python_callable=insert_update_db,
    provide_context=True,
    dag=dag,
)


def send_message(**kwargs):
    try:
        ti = kwargs['ti']
        correo = ti.xcom_pull(task_ids='get_db_connection_info')[
            'credential_gmail']
        df = ti.xcom_pull(task_ids='read_csv')
        x = smtplib.SMTP('smtp.gmail.com', 587)
        x.starttls()
        x.login(correo['gmail'], correo['password'])

        # Resto del código para enviar el mensaje
        valor_uf = df[df['codigo'] == 'uf']['valor'].values[0]
        fecha = df[df['codigo'] == 'uf']['fecha'].values[0]
        if (valor_uf > 36000):
            subject = 'UF Sobrepasa el valor'
            body_text = f'El valor de la UF es {valor_uf} para la fecha = {fecha}'
            message = 'Subject: {}\n\n{}'.format(subject, body_text)
            x.sendmail(correo['gmail'],
                       correo['gmail'], message)
            print('Mensaje exitoso')
    except Exception as exception:
        print(exception)
        print('Failure')


send_message_task = PythonOperator(
    task_id='send_message',
    python_callable=send_message,
    provide_context=True,
    dag=dag,
)


# Definir el orden de ejecución de las tareas
fetch_data_task >> write_to_csv_task >> read_csv_task >> get_db_connection_info_task >> insert_update_db_task >> send_message_task
