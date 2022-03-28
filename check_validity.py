"""
Updates intrasis_archive_tasks in urdar_overseer with information about spatial objects with validity issues. 
Gives a count of the number of spatial features with issues, as well as an array of their Object IDs.
@author: gislipals
"""
import pandas as pd
from decouple import config
from urdar_dev import backend as db

# connection details
dbname = "urdar_overseer"
password = config("PASS")
con, eng = db.urdar_login(pg_dbname=dbname, password=password)
c = 0

dfdbases = pd.read_sql("SELECT site_archive from intrasis_archive_tasks where invalid_count is null", con)
dbases_all = dfdbases["site_archive"].tolist()

for dbase in dbases_all:
    # Prints a running count of the number of databases processed
    print(c)
    # The validity query. Uses ST_ISVALID and ARRAY_AGG to count invalid features and aggregate their IDs
    dfvalid = pd.read_sql(   
        """
        SELECT
        count(*) as invalid_count,
        coalesce(array_to_string(array_agg("ObjectId"::text),','),'No invalid objects found') as invalid_objects
        from {0}."GeoObject" 
        where st_isvalid(the_geom) is False
        """.format(
                dbase),con)
    # Writes the results to the table intrasis_archive_tasks            
    with con.cursor() as cur:
        cur.execute(
            """
            UPDATE public.intrasis_archive_tasks
            SET invalid_count = {} WHERE site_archive like '{}';
            UPDATE public.intrasis_archive_tasks
            SET invalid_objects = '{}' WHERE site_archive like '{}';
            """.format(dfvalid.invalid_count.values[0],dbase,dfvalid.invalid_objects.values[0],dbase)
        )
    c += 1