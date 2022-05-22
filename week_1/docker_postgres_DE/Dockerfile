FROM python:3.9


RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install argparse
RUN pip install flask-sqlalchemy 
RUN pip install psycopg2

WORKDIR /app
COPY pipeline.py pipeline.py 

ENTRYPOINT ["python","pipeline.py"]

