FROM apache/airflow:2.9.1 

RUN pip install --upgrade pip
WORKDIR /opt/airflow
COPY requirements.txt .
RUN pip install -r requirements.txt