FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_hwdata.py ingest_hwdata.py 

ENTRYPOINT [ "python", "ingest_hwdata.py" ]