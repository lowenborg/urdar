"""#! C:\\Python38\\python.exe"""
# ---------------------------------------------
# Extractions of flat files from Intrasis archives
# ---------------------------------------------
# Install dependencies
import pandas as pd
from urdar_dev import backend as db
from urdar_dev import automate as auto
from decouple import config

pg_dbname = "urdar_overseer"
password = config("PASS")
connection, engine = db.urdar_login(pg_dbname, password)
intrasis_databases = pd.read_sql(
    "SELECT datname FROM pg_database where datname not in ('"
    + "', '".join(db.system_dbs)
    + "') EXCEPT select site_archive from public.intrasis_archives",
    connection,
)

# load functions. Only necessary if the underlying SQL functions used in the Python function calls are altered.
for i in intrasis_databases["datname"]:
    auto.function_loader(i, password)

# extract site data
for i in intrasis_databases["datname"]:
    auto.intrasis_site_data(i, password)

# extract find count data
for i in intrasis_databases["datname"]:
    auto.find_count(i, password)

# extract every attribute
for i in intrasis_databases["datname"]:
    auto.geo_obj_attrvals(i, password)

# count postholes
for i in intrasis_databases["datname"]:
    auto.count_features(i, password)

# count nodes and edges
for i in intrasis_databases["datname"]:
    auto.count_edge_nodes(i, password)
