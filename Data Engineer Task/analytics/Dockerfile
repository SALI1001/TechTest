FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy pandas pymssql
WORKDIR /app
COPY . .
CMD [ "python", "./analytics.py" ]