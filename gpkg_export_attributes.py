from osgeo import ogr
import os
import pandas as pd
import geopandas as gpd
from urdar_dev import backend as db
import os
from pathlib import Path
from decouple import config

# connection details
host = "130.238.10.179"
username = "urdar_daemon"
dbname = "urdar_overseer"
password = config("PASS")
version = 10
port = 5432
con, eng = db.urdar_login(pg_dbname=dbname, password=password)

# find which gpkgs have already been made
full_path = Path(
    "D:/Dropbox/palsson_analytics/urdar/python_code/output/gpkg/v{}".format(version)
)
completed_exports = [
    os.path.splitext(filename)[0] for filename in os.listdir(full_path)
]
dfdbases = pd.read_sql("SELECT site_archive from intrasis_archive_tasks WHERE gpkg_version < {} limit 500".format(version), con)
dbases_all = dfdbases["site_archive"].tolist()

c = 1

for dbase in dbases_all:
    print("scanning {}".format(dbase))

    try:
        # Define SQL for site attribute (1 row) and any relations stored in the database.
        sql_siteatt = """
        select
        row_number() over (order by site.site_name) as pk,
        site.site_name,
        site.site_landskap,
        site.lan_name,
        site.parish_name,
        site.geom,
        siteatt.slutdatum,
        siteatt.undersokningstyp,
        siteatt.exploateringstyp,
        siteatt.projektnamn,
        siteatt.projektkod,
        siteatt.raa_dnr,
        siteatt.lst_dnr,
        siteatt.plats,
        siteatt.undersöknings_id

        from intrasis_archives site
        left join intrasis_archives_unnested_attributes siteatt on '{}' = siteatt.site_archive
        where site.site_archive like '{}'
        """.format(
            dbase, dbase
        )

        # Create DataFrame from site attributes.
        df = gpd.read_postgis(sql_siteatt, con)
        df.to_file(
            "output/gpkg/v{}/{}.gpkg".format(version, dbase),
            layer="project_information",
            driver="GPKG",
        )
    except Exception as e:
        print(e)
        print("Site attribute table creation failed.")    
    try:
        # relation query
        sql_rels = """
        select
        null as geom,
        "ParentId" as parent_id, 
        "ChildId" as child_id, 
        "ParentText" as parent_text,
        "ChildText" as child_text
        from {0}."ObjectRel" inner join {0}."RelationDef" on "ObjectRel"."MetaId" = "RelationDef"."MetaId" 
        where "ParentText" not like 'Skapar'
        """.format(
            dbase
        )

        gdf = gpd.read_postgis(sql_rels, con)
        gdf.to_file(
            "output/gpkg/v{}/{}.gpkg".format(version, dbase),
            layer='object_relations',
            driver="GPKG",
        )

    except Exception as e:
        print(e)
        print("Relation table creation failed.")

    try:
        # Group query
        sql_groups = """
        with aspatial as (
        select "ObjectId" from {0}."Object" where "ClassId" not in (3,19,21,14,15)

        Except

        select "ObjectId" from {0}."GeoRel" union select "GeoObjectId" from {0}."GeoRel"
        ),

        spatial as (
        select "ObjectId" from {0}."GeoRel"
        ),

        descr as (
        select b."ObjectId" as object_id,a.converted_text
        from {0}."FreeText" a
				inner join {0}."Attribute" b on a."AttributeId" = b."AttributeId")

        select distinct on (a."ObjectId") 
        a."ObjectId" as group_id,
        d."Name" as group_name,
        e.converted_text,
        null as geom
        from aspatial a 
        inner join {0}."ObjectRel" c on a."ObjectId" = c."ParentId"
        inner join spatial b on c."ChildId" = b."ObjectId"
        inner join {0}."Object" d on a."ObjectId" = d."ObjectId"
        inner join descr e on a."ObjectId" = e.object_id
        """.format(dbase)

        gdf = gpd.read_postgis(sql_groups, con)
        gdf.to_file(
            "output/gpkg/v{}/{}.gpkg".format(version, dbase),
            layer='groups',
            driver="GPKG",
        )
    except Exception as e:
        print(e)
        print("Group table creation failed.")

    try:
        sql_att = """
        select
        null as geom,
        aa."ObjectId" as object_id,
        aa."Label" as attribute_label,
        ab."Value" || case when ab."Unit" is not null or ab."Unit" not like '' then ' ' || ab."Unit" else null end as attribute_value
    	from {0}."Attribute" aa
        inner join {0}."AttributeValue" ab on aa."AttributeId" = ab."AttributeId"
	    inner join {0}."GeoRel" ac on aa."ObjectId" = ac."ObjectId"
        """.format(dbase)

        gdf = gpd.read_postgis(sql_att, con)
        gdf.to_file(
            "output/gpkg/v{}/{}.gpkg".format(version, dbase),
            layer='attributes',
            driver="GPKG",
        )
    except Exception as e:
        print(e)
        print("Attribute table creation failed.")

    print(c)
    c+= 1

    # Update task list
    with con.cursor() as cur:
            cur.execute(
        """
        UPDATE intrasis_archive_tasks SET gpkg_version = {} WHERE site_archive LIKE '{}'
        """.format(
                version,dbase))