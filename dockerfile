FROM apache/airflow:2.2.3

# Copy the .env file into the working directory
COPY .env /usr/local/airflow/.env

# Install required packages
RUN pip install pymongo requests pandas psycopg2 python-dotenv
