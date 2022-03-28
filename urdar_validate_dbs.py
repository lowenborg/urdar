"""
This script validates databases, for instance by checking if two databases with the same name in different dump files are identical
author: gisli.palsson@gmail.com
"""
# ---------------------------------------------
# Extractions of flat files from Intrasis archives
# ---------------------------------------------
# Install dependencies
import pandas as pd
from urdar_dev import backend as db
import datetime as dt
from decouple import config

pg_dbname = 'postgres'
password = config('PASS')
connection, engine = db.urdar_login(pg_dbname,password)
intrasis_databases = pd.read_sql("SELECT datname FROM pg_database where datname not in ('" + "', '".join(db.system_dbs)+"')", connection)

# initialize an empty DataFrame which is then populated using the function 'intrasis_site_data' from the urdar_dev module
blob_metrics = pd.DataFrame()

def urdar_blob_metrics(dbname,password):
    """calculate some aspects of BLOB objects in an Intrasis database
    author: gisli.palsson@gmail.com
    """
    # connect to the server
    connection, engine = db.urdar_login(dbname,password)
    # Create a cursor object
    cursor = connection.cursor()
    # blob size
    blob_sql = pd.read_sql("""
    select current_database(),count("Value") as blob_count, pg_size_pretty(pg_total_relation_size('"BinaryAttributeValue"') ) as blob_storage_size from "BinaryAttributeValue"
    """,connection)
    global blob_metrics 
    # add the result to the blob metrics list
    blob_metrics = pd.concat([blob_metrics,blob_sql])
    return blob_metrics
    # closes the connection to the database
    cursor.close()
    

# fire off the function
for i in intrasis_databases['datname']:
    urdar_blob_metrics(i,password)


# write output
blob_metrics.to_csv('output\\csv\\blob_metrics.csv', index=False)
blob_metrics.to_json('output\\json\\blob_metrics.json', orient='table',index=False)    


# write blobs to image files
db_for_blob_export = 's2008039' # provide a list to iterate through many databases
connection, engine = db.urdar_login(db_for_blob_export,password)

# images will be labeled from 1 upwards. Can also change to extract data from the blob table itself. To be discussed in the team if this function
# ever gets used for much beside testing
blobs = pd.read_sql('select * from "BinaryAttributeValue"',connection)
c = 1
for i in blobs['Value']:
    try:
        open(f'output\\blobs\\{pg_dbname}\\{str(c)}.jpg', 'wb').write(i)
        c = c+1
    except:
        pass

for i in blobs['Value']:
    print(i)

for i in intrasis_databases['datname']:
    connection, engine = db.urdar_login(i,password)
    # Create a cursor object
    attrs = pd.read_sql("""select "MetaId","Name" from "Definition" where "MetaId" in (183,9903,10047,10174,10175)""",engine)
    print(f'Scanning {i}')
    print(attrs.head())