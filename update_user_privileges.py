"""
Script to update user privileges in all databases at once.
author:gisli.palsson@gmail.com
"""
##############
# Dependencies
##############
import pandas as pd
from urdar_dev import backend as db
from urdar_dev import automate as auto
from decouple import config

password = config("PASS")
##################################
# Create function
##################################
def update_user_privileges(
    role=input("Enter the role to be updated: "), password=password
):
    """Updates privileges for a user. To be expanded later with more options.
    author:@gislipals"""
    con, eng = db.urdar_login(pg_dbname="urdar_overseer", password=password)
    intrasis_databases = pd.read_sql(
        "SELECT datname FROM pg_database where datname not in ('"
        + "', '".join(db.system_dbs)
        + "') order by datname",
        con,
    )
    # update privileges in overseer
    with con.cursor() as cur:
        print("Updating privileges for role {} in database urdar_overseer".format(role))
        cur.execute(
            f"""
            -- Grant access to current tables and views
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role};
            -- Ensure privileges to future tables
            ALTER DEFAULT PRIVILEGES
                GRANT SELECT
            ON TABLES 
            TO {role};
            -- sequences
            GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO {role};
            ALTER DEFAULT PRIVILEGES
                GRANT SELECT, USAGE
            ON SEQUENCES 
            TO {role};"""
        )
    # update privileges in Intrasis databases
    for i in intrasis_databases["datname"]:
        con, eng = db.urdar_login(pg_dbname=i, password=password)
        print("Updating privileges for role {} in database {}".format(role, i))
        with con.cursor() as cur:
            cur.execute(
                f"""
            -- Grant access to current tables and views
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role};
            -- Ensure privileges to future tables
            ALTER DEFAULT PRIVILEGES
                GRANT SELECT
            ON TABLES 
            TO {role};

            -- sequences
            GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO {role};
            ALTER DEFAULT PRIVILEGES
                GRANT SELECT, USAGE
            ON SEQUENCES 
            TO {role};"""
            )


##############
# Run function
##############
if __name__ == "__main__":
    update_user_privileges()
