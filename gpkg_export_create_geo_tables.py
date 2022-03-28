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

dfdbases = pd.read_sql("SELECT site_archive from intrasis_archive_tasks where table_created is null limit 500", con)
dbases_all = dfdbases["site_archive"].tolist()

c = 1

for dbase in dbases_all:
    print("scanning {}".format(dbase))
    try:
        with con.cursor() as cur:
            sql = """
            create table if not exists "temp".{0}_features as
            with objs as (
            SELECT distinct on (a."ObjectId")
            a."ObjectId" as object_id,
            a."PublicId" as b_id,
            a."ClassId" as object_class_id,
            a."SubClassId" as object_subclass_id,
            a."Name" as object_name,
            f."Name" as class,
            e."Name" as subclass,
            c."ObjectId" as geoobject_id,
            st_curvetoline(st_makevalid(c.the_geom)) as geom,
            c."MetaId" as geoobject_meta_id,
            c."SymbolId" as geoobject_symbol_id,
            d."Type" as spatial_type
            from {0}."Object" a
            inner join {0}."GeoRel" b on a."ObjectId" = b."ObjectId"
            left join {0}."GeoObject" c on b."GeoObjectId" = c."ObjectId"
            left join {0}."GeoObjectDef" d on c."MetaId" = d."MetaId"
            left join {0}."Definition" e on a."SubClassId" = e."MetaId"
            left join {0}."Definition" f on a."ClassId" = f."MetaId"
            ),

            attrs as (
            select
            aa."AttributeId" as attribute_id,
            aa."MetaId" as attribute_meta_id,
            aa."ObjectId" as object_id,
            aa."Label" as attribute_label,
            ab."AttributeValueId" as attributevalue_id,
            ab."Value" || case when ab."Unit" is not null or ab."Unit" not like '' then ' ' || ab."Unit" end as attributevalue_value,
            ag."Name" as definition_name,
            ag."Description" as definition_description,
            ag."VersionReferenceId" as definition_versionreference,
            ah."Value" as alternative_def_value,
            aa."Label" || ': ' || (case when ab."Value" is null or ab."Value" like '' then null else ab."Value" end) || case when ab."Unit" is not null then ' ' || ab."Unit" end as attributevalue
            from {0}."Attribute" aa
            inner join {0}."AttributeValue" ab on aa."AttributeId" = ab."AttributeId"
            inner join {0}."AttributeDef" ac on aa."MetaId" = ac."MetaId"
            left join {0}."Definition" ag on ac."MetaId" = ag."MetaId"
            left join {0}."AlternativeDef" ah on ac."MetaId" = ah."MetaId"
            ),

            attrarray as (select object_id, array_agg(attributevalue) as attr_array from attrs group by object_id),
                
            grp as (
            select a."ObjectId" as group_id,a."Name" as group_name, b."ChildId" as object_id 
            from {0}."Object" a
            left join {0}."ObjectRel" b on a."ObjectId" = b."ParentId"
            where a."SubClassId" in (52,10394,9906)
            ),
                
            descr as (
            select a.converted_text,b."ObjectId" as object_id from {0}."FreeText" a
            left join {0}."Attribute" b on a."AttributeId" = b."AttributeId"
            )
                
            select
            row_number() over (order by objs.object_id) as pk,
            '{0}' as intrasis_archive,
            objs.object_id, 
            objs.b_id,
            objs.object_name,
            objs.class,
            objs.subclass,
            st_force2d(objs.geom) as geom,
            objs.spatial_type,
            array_to_string(attrarray.attr_array, ',') as attributes,
            object_descr.converted_text as object_description,
            grp.group_name,
            group_descr.converted_text as group_description

            from objs
            left join attrarray on objs.object_id = attrarray.object_id
            left join public.geo_objs_groupby_geom geo_objs on objs.geom = geo_objs.the_geom
            left join grp on objs.object_id = grp.object_id
            left join descr group_descr on grp.group_id = group_descr.object_id
            left join descr object_descr on objs.object_id = object_descr.object_id;
            create index if not exists gix_{0} on "temp".{0}_features using GIST(geom)
            """.format(dbase)
            cur.execute(sql)
            with con.cursor() as cur:
                cur.execute(
                """
                UPDATE intrasis_archive_tasks SET table_created = True WHERE site_archive LIKE '{}'
                """.format(
                dbase))
    except Exception as e:
        print(e)
        print("Feature table creation failed.")
    
    print(c)
    c+= 1
    # Update task list
    