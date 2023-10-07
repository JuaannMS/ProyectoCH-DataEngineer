# ProyectoCH-DataEngineer

Proyecto final realizado con tecnologías y lenguajes vistos en el curso de Data Engineering Flex.

Se estableció un pipeline de datos para realizar una ETL, extrayendo información de una API pública que incluía indicadores económicos de Chile, como el valor de la UF y el dólar, entre otros. Posteriormente, estos datos se procesaron y almacenaron en una base de datos de Amazon Redshift. Este proceso se automatizó mediante un flujo de trabajo creado en Apache Airflow. Además de las tareas mencionadas, se configuró una notificación para alertar mediante Gmail cuando el valor de la UF superara los 36000 CLP. Todo esto se llevó a cabo utilizando un contenedor de Docker.


En este proyecto se utilizaron tecnologías como:

- Docker
- Apache Airflow
- Python (bibliotecas Pandas, Requests, SQLAlchemy)
- Amazon Redshift
- DBeaver

- Para almacenar los datos en RedShift, CoderHouse nos proporcionó una cuenta donde se creó la base de datos utilizando DBeaver.
- Para acceder al proyecto desde Docker, es necesario crear una nueva imagen y luego lanzarla en un puerto específico.
- Para acceder al DAG en Airflow, se debe crear una cuenta con los siguientes parámetros:

- airflow users create \
          --username  \
          --firstname  \
          --lastname  \
          --role Admin \
          --email admin@example.org
