FROM apache/airflow:2.5.1
#COPY . /app
#RUN pip install openpyxl
COPY requirements.txt .
RUN pip install -r requirements.txt


