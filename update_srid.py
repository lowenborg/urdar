# import dependencies
from urdar_dev import automate as auto
from urdar_dev import backend as db
import pandas as pd
from decouple import config

# connect to the server to compile a list of Intrasis databases
password = config("PASS")
con, eng = db.urdar_login("urdar_overseer", password)
intrasis_databases = pd.read_sql(
    "SELECT site_archive,likely_srid FROM intrasis_archives order by site_archive", con
)

for j, i in intrasis_databases.iterrows():
    # connect to target server
    con, eng = db.urdar_login(i[0], password)
    # create a cursor object
    with con.cursor() as cur:
        # print database and srids
        print(f"Reprojecting geometries in {i[0]} from {i[1]} to 3006")
        cur.execute(
            f"""
        update "GeoObject" set the_geom = st_transform(st_setsrid(the_geom,{i[1]}::int),3006)
        """
        )
