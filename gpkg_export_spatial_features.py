import pandas as pd
import geopandas as gpd
import os
from shapely import wkt
from decouple import config
from urdar_dev import backend as db
from pathlib import Path
import fiona

# connection details
host = "(INSERT)"
username = "urdar_daemon"
dbname = "urdar_overseer"
password = config("PASS")
version = 10
port = 5432
c = 1
con, eng = db.urdar_login(pg_dbname=dbname, password=password)

dfdbases = pd.read_sql("SELECT site_archive from intrasis_archive_tasks WHERE gpkg_version < {}".format(version), con)
dbases_all = dfdbases["site_archive"].tolist()

# Filter out complete database exports
dbases_incomplete = []
for dbase in dbases_all:
    try:
        gpkg = "output/gpkg/v{}/{}.gpkg".format(version, dbase)
        layers = fiona.listlayers(gpkg)
        if "features" not in layers:
            dbases_incomplete.append(dbase)
        else:
            pass
    except:
        pass

for dbase in dbases_incomplete:
    print("Scanning {}".format(dbase))
    try:
        sql_features = """SELECT * FROM "temp".{}_features""".format(dbase)
        gdf = gpd.read_postgis(sql_features, con)
        #gdf.plot()
        if len(gdf) > 0:
            gdf.to_file(
            "output/gpkg/v{}/{}.gpkg".format(version,dbase),
            layer="features",
            driver="GPKG")
        else:
            pass
    except Exception as e:
        print(e)
        print('{} export failed'.format(dbase))
    
    print(c)
    c+= 1