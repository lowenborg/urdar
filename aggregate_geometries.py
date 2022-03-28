"""
Populates tables in overseer with information about every GeoObject in Urdar, as well as whether there are identical or intersecting geometries across different projects
@author: gislipals
"""
##############
# Dependencies
##############
import pandas as pd
from urdar_dev import backend as db
from urdar_dev import automate as auto
from decouple import config

###########
# Login and get a list of databases
###########
pg_dbname = "urdar_overseer"
password = config("PASS")
con, eng = db.urdar_login(pg_dbname, password)
intrasis_databases = db.intrasis_databases(con)

def main():
    # extract site data
    for dbname in intrasis_databases["datname"]:
        # Uses DBLink to query every database and stores an INSERT statement in a variable.
        # The INSERT statement is then executed to populate the table urdar_overseer.geo_objs_all
        insert = """insert into geo_objs_all(intrasis_archive,object_ids,occurrence,the_geom)
        SELECT * from dblink('dbname={0}',
        'select ''{0}'' as intrasis_archive,array_agg("ObjectId") as object_ids,
        count(*) as occurrence,the_geom from "GeoObject" group by the_geom')
        as t(
        intrasis_archive varchar,
        object_ids _int4,
        occurrence int4,
        the_geom geometry)
        """.format(
            dbname
        )
        auto.populate_overseer_table(insert, dbname, password)

if __name__ == "__main__":
    main()
