import pymysql
import csv
import boto3
import configparser 

#initializing a connection to mySQL database
parser_sql = configparser.ConfigParser()
parser_sql.read("pipeline.conf")
hostname = parser_sql.get("mysql_config", "hostname")
port = parser_sql.get("mysql_config", "port")
username = parser_sql.get("mysql_config", "username")
dbname = parser_sql.get("mysql_config", "database")
password = parser_sql.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
user=username,
password=password,
db=dbname,
port=int(port)
)

if conn is None:
    print("Error connecting to MySQL database")
else:
    print("MYSQL connection established")


#Full extraction of the books tabe and writing into a delimited csv file
m_query = "SELECT * FROM books;"
local_filename = "books_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
result = m_cursor.fetchall()

with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp,delimiter='|')
    csv_w.writerows(result)

fp.close()
m_cursor.close()
conn.close()


#loading the aws boto credentials and loading them into the s3 buckets
parser_bucket = configparser.ConfigParser()
parser_bucket.read("pipeline.conf")
access_key = parser_bucket.get("aws_boto_credentials", "access_key")
secret_key = parser_bucket.get("aws_boto_credentials", "secret_key")
bucket_name = parser_bucket.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client('s3',
aws_access_key_id = access_key,
aws_secret_access_key=secret_key,
)

s3_file = local_filename
s3.upload_file(local_filename,bucket_name,s3_file)










