"""
# Main codebase for the Urdar restore and extract program.
author:gisli.palsson@gmail.com
"""
# Dependencies
import os
import pandas as pd
from pathlib import Path
import re
from urdar_dev import backend as db
import glob
import traceback
from decouple import config

# database name for the connection.
password = config("PASS")
pg_dbname = "postgres"
connection, engine = db.urdar_login(pg_dbname, password)

# ----------------------------------
# list all dump files and create some variables used later
# ----------------------------------

# set the dump folder path
# dump_folder = Path("../restoring_dumps/all_dumps")
full_path = Path("D:/cloud_apps/Box/Urdar/DBackup")

# fix some issues with dump file names, such as the '-' character and problematic substrings like '.zip' inside the file name
for f in glob.glob("D:/cloud_apps/Box/Urdar/DBackup/*"):
    new_filename = f.replace("-", "_").replace(".zip", "")
    os.rename(f, new_filename)

# create a list of every file path
all_dumps = []
with os.scandir(full_path) as folder:
    for entry in folder:
        if entry.name.endswith(".dump") and entry.is_file():
            all_dumps.append(entry.path)
        if entry.name.endswith(".backup") and entry.is_file():
            all_dumps.append(entry.path)

# ---------------------------------------------
# Create every database that needs to be created to receive incoming dump files. This avoids some errors in creating them directly from the dump file.
# Derives a set of CREATE DATABASE commands by comparing the names of the source file dumps to names of existing databases
# on the server. This is necessary as PostgreSQL does not have SQL Server's 'CREATE IF NOT EXIST' functionality.
# After that we batch execute them with AUTOCOMMIT = ON.
# Note: This has changed a bit as it originally only created databases that did not already exist. It is possible that two DBs of the same
# name have differences in data. Currently creates '_v2' versions of pre-existing databases.
# ---------------------------------------------
all_source_db_names = []
with os.scandir(full_path) as folder:
    for entry in folder:
        if entry.name.endswith(".dump") and entry.is_file():
            all_source_db_names.append(entry.name[0:-5].lower())
        if entry.name.endswith(".backup") and entry.is_file():
            all_source_db_names.append(entry.name[0:-7].lower())

# Create a cursor object
cursor = connection.cursor()

# Create a variable with a query for every existing DB name
listall_db = "SELECT datname FROM pg_database"

# Execute and create a list from the query
cursor.execute(listall_db)
all_target_db_names = list(cursor.fetchall())

# Initial list is created with a ',' after every database name, so we update the variable with a [-1] slice in an iteration.
all_target_db_names = [dbname[-1] for dbname in all_target_db_names]

# This is the difference between databases needed and databases already existing.
db_creation_list = [
    dbname for dbname in all_source_db_names if dbname not in all_target_db_names
]

# Next, we generate a set of SQL query strings from db_creation_list
db_creation_commands = []
for dbname in db_creation_list:
    db_creation_commands.append(
        "CREATE DATABASE {0} WITH TEMPLATE = urdar_template ENCODING = 'UTF8'".format(
            dbname
        )
    )

# Finally, we execute the commands. Commented out right now to avoid sending blank queries to the server. db_creation_commands and _list
# will be empty if all the databases have already been made
# print statement to insert '_2' in case a database of that name already exists
# Currently NOT versioning existing databases. Just ignoring ones that already exist.

for command in db_creation_commands:
    try:
        cursor.execute(command)
        print(f"Command '{command}' executed successfully")
    except Exception:
        # cursor.execute(command[:-49] + '_v2' + command[-49:])
        print(
            "Error in database creation"
        )  # Command changed to: '{command[:-49] + '_v2' + command[-49:]}'")
        traceback.print_exc()

# ---------------------------------------------
# Create a list of pg_restore commands to populate the databases.
# this is how a psql pg_dump command should look like:
# pg_restore -h archviz.humlab.umu.se -p 5432 -U daniel -d E20061009 -O -x -c --no-security-labels D:\Dropbox\palsson_analytics\urdar\restoring_dumps\all_dumps\E20061009.dump
# uses regex to slice the correct substrings from the dump names
# ---------------------------------------------
pq_restore = Path("C:/Program Files/PostgreSQL/12/bin>pg_restore")

shell_commands = []
for dump in all_dumps:
    shell_commands.append(
        "pg_restore -h (INSERT) -p 5432 -U urdar_daemon -d "
        + re.search(r"(?<=DBackup\\)(.*)(?=\.)", dump).group(0).lower()
        + " -c -w -O -x --no-security-labels "
        + str(full_path)
        + "\\"
        + re.search(r"(?<=DBackup\\)(.*)", dump).group(0)
    )

cursor.close()
# ---------------------------------------------
# Create a batch file with all of the db creation commands
# ---------------------------------------------
pg_restore_bat = open(
    r"D:\Dropbox\palsson_analytics\urdar\restoring_dumps\urdar_restore_dbs.bat", "w+"
)

pg_restore_bat.write(
    r"""ECHO OFF
cd /d C:\Program Files\PostgreSQL\12\bin
set "PGPASSWORD=OPENBATCHFILE_AND_ENTER_IT_HERE" """
    + "\n"
)

for item in shell_commands:
    pg_restore_bat.write("%s\n" % item)

pg_restore_bat.write("Pause")
pg_restore_bat.close()
