"""
Compiles a list of Intrasis databases on the server and creates foreign data wrappers for them in the overseer database.
author:gisli.palsson@gmail.com
"""
##############
# Dependencies
##############
import pandas as pd
from urdar_dev import backend as db
from urdar_dev import automate as auto

##################################
# Create function
##################################
def create_fdw_user(
    password=input("Enter password for urdar_daemon: "),
    user=input("Enter the user to be mapped: "),
    userpass=input("Enter the user's password: "),
):
    """Creates a user map from urdar_overseer to every Intrasis database on the server
    author:@gislipals"""
    con, eng = db.urdar_login(pg_dbname="urdar_overseer", password=password)
    intrasis_databases = pd.read_sql(
        "SELECT datname FROM pg_database where datname not in ('"
        + "', '".join(db.system_dbs)
        + "') order by datname",
        con,
    )
    for i in intrasis_databases["datname"]:
        print("Creating foreign data wrapper for {}".format(i))
        with con.cursor() as cur:
            cur.execute(
                """CREATE USER MAPPING IF NOT EXISTS FOR {} SERVER {} OPTIONS (user '{}', password '{}');""".format(
                    user, i, user, userpass
                )
            )


##############
# Run function
##############
if __name__ == "__main__":
    create_fdw_user()
