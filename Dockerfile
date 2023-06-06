FROM apache/spark-py:v3.3.0
WORKDIR /app
USER root
RUN apt-get update
RUN apt-get install -y python3 gcc g++ build-essential make curl
RUN curl https://jdbc.postgresql.org/download/postgresql-42.3.6.jar --output ./utils/postgresql.jar
RUN cp ./utils/postgresql.jar /opt/spark/jars/postgresql-42.3.6.jar
RUN pip install --upgrade pip
COPY . .
RUN pip install -r requirements.txt


CMD [ "python3", "main.py" ]