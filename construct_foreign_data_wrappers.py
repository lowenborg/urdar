"""
Compiles a list of Intrasis databases on the server and creates foreign data wrappers for them in the overseer database.
author:gisli.palsson@gmail.com
"""
##############
# Dependencies
##############
import pandas as pd
from urdar_dev import backend as db
from decouple import config

############
# Parameters
############
password = config("PASS")
##################################
# Create function
##################################
def create_fdw():
    """author:@gislipals"""
    con, eng = db.urdar_login(pg_dbname="urdar_overseer", password=password)
    intrasis_databases = pd.read_sql(
        "SELECT datname FROM pg_database where datname not in ('"
        + "', '".join(db.system_dbs)
        + "') EXCEPT SELECT site_archive from intrasis_archive_tasks WHERE foreign_wrapper = True",
        con,
    )
    for i in intrasis_databases["datname"]:
        print("Creating foreign data wrapper for {}".format(i))
        with con.cursor() as cur:
            cur.execute(
                """
            CREATE SERVER IF NOT EXISTS {0} FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '130.238.10.179', dbname '{0}', port '5432');
            DROP SCHEMA IF EXISTS {0} CASCADE;
            CREATE SCHEMA {0};
            IMPORT FOREIGN SCHEMA public FROM SERVER {0} INTO {0};
            UPDATE intrasis_archive_tasks SET foreign_wrapper = True WHERE site_archive LIKE '{0}'
            """.format(
                    i
                )
            )
    cur.close()

##############
# Run function
##############
if __name__ == "__main__":
    create_fdw()
