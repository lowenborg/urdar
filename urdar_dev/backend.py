"""
Uppsala University 2020
Description: This module contains backend functionality for the Urdar database servers.
@author: gisli.palsson@gmail.com
"""
# ---------------------------------------------
# Contains connection parameters for Urdar's backend
# ---------------------------------------------
import psycopg2
from sqlalchemy import create_engine
import pandas as pd


def urdar_login(pg_dbname, password, user="urdar_daemon"):
    """
    Uppsala University 2020
    Description: This function provides login functionality for the Urdar backend.
    @author: gisli.palsson@gmail.com
    """
    pg_host = "130.238.10.179"
    pg_port = "5432"
    connection = psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_dbname, user=user, password=password
    )
    connection.autocommit = True
    engine = create_engine(
        "postgresql://" + user + ":" + password + "@130.238.10.179/" + pg_dbname
    )
    return connection, engine


# code for creating tables
def create_table(table, table_name, engine):
    table.to_sql(table_name, engine, index=True)


# code for adding privileges
def add_privileges_and_pk(table_name, cursor):
    """
    Uppsala University 2020
    Description: Adds a primary key and group-wide privileges to a postgresql table
    @author: gisli.palsson@gmail.com
    """
    # put a primary key on the table
    pk = "ALTER TABLE " + table_name + " ADD pi_id serial PRIMARY KEY"
    # grant privileges
    privileges_urd = "GRANT ALL PRIVILEGES ON " + table_name + ''' TO "urdar_devs"'''
    # execute the commands
    cursor.execute(pk)
    cursor.execute(privileges_urd)


# hardcoded list of system databases used to filter out databases with no Intrasis data
system_dbs = [
    "postgres",
    "template0",
    "template1",
    "urdar_reference_data",
    "urdar_template",
    "postgis_db",
    "postgis20",
    "template_postgis_20",
    "urdar_overseer",
    "intrasislicense",
    "jens",
    "m20007",
    "m200123",
    "v20012",
]

# function for counting databases
def intrasis_databases(connection):
    dbs = pd.read_sql(
        "SELECT datname FROM pg_database where datname not in ('"
        + "', '".join(system_dbs)
        + "') order by datname",
        connection,
    )
    return dbs


##################################
# Overseer functions
#################################
site_attr_func = """
DROP FUNCTION site_attributes();
CREATE FUNCTION site_attributes() 
    RETURNS TABLE (
		site_name	varchar,
		table_array	information_schema.sql_identifier[],
		table_ct	int8,
		db_creation_date	text,
		start_of_fieldwork	text,
		end_of_fieldwork	text,
		site_archive	name,
		project_ct	int8,
		omrade_schakt_ct	int8,
		arch_obj_ct	int8, 
		fornlamn_ct	int8,
		sample_ct	int8,
		find_ct	int8,
        point_ct int8,
        polygon_ct int8,
        polyline_ct int8,
		parish_name	text,
		lan_name	text,
		geom_centroid_wkt	text,
		x_dispersion	float8,
		y_dispersion	float8,
		intrasis_srid	int4,
        likely_srid int4,
		site_attributes text[],
		geom_centroid "public"."geometry",
        eez_name text) AS 
$func$
BEGIN
RETURN QUERY 
with st_collection as (select st_union(st_centroid(st_makevalid(the_geom))) as geom from "GeoObject"),

proj as (select geo.geom,3006 as likely_srid
--case 
--    when ST_Intersects(
--        ST_GeometricMedian(st_transform(
--            ST_SetSRID(geo.geom,
--                 case when
--                    (SELECT "AttributeValue"."Value"::integer as srid FROM "Attribute" INNER JOIN "AttributeValue" ON "Attribute"."AttributeId" = "AttributeValue"."AttributeId" where "Attribute"."MetaId" = 489 LIMIT 1) = 0 then 3006 else
--                    (SELECT "AttributeValue"."Value"::integer FROM "Attribute" INNER JOIN "AttributeValue" ON "Attribute"."AttributeId" = "AttributeValue"."AttributeId" where "Attribute"."MetaId" = 489 LIMIT 1) end)
--                    ,3006)),eez.geom) ='t' 
--                    then case when
--                        (SELECT "AttributeValue"."Value"::integer as srid FROM "Attribute" INNER JOIN "AttributeValue" ON "Attribute"."AttributeId" = "AttributeValue"."AttributeId" where "Attribute"."MetaId" = 489 LIMIT 1) = 0 then 3006 else
--                        (SELECT "AttributeValue"."Value"::integer FROM "Attribute" INNER JOIN "AttributeValue" ON "Attribute"."AttributeId" = "AttributeValue"."AttributeId" where "Attribute"."MetaId" = 489 LIMIT 1) end    
--    when 
--        ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,3006),3006)),eez.geom) ='t' then 3006
--    when 
--        ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,3021),3006)),eez.geom) ='t' then 3021
--    when 
--        ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,3010),3006)),eez.geom) ='t' then 3010
--    when 
--        ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,3007),3006)),eez.geom) ='t' then 3007
--    when 
--        ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,3027),3006)),eez.geom) ='t' then 3027
--    when 
--        ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,3008),3006)),eez.geom) ='t' then 3008        
--    when 
--        ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,4124),3006)),eez.geom) ='t' then 4124
    --when 
    --    ST_Intersects(ST_GeometricMedian(st_transform(st_setsrid(geo.geom,4308),3006)),eez.geom) ='t' then 4308            
--    else  
--        3006 end as likely_srid
from st_collection geo,dblink('dbname=urdar_reference_data','SELECT st_union(geom) as geom FROM eez_sweden') AS eez(geom public.geometry)
),

cent as (
    select 
    1 as rownum,
    ST_GeometricMedian(st_transform(st_setsrid(proj.geom,proj.likely_srid),3006)) as site_centroid_wkb,
    ST_AsText(ST_GeometricMedian(st_transform(st_setsrid(proj.geom,proj.likely_srid),3006))) as site_centroid_wkt,
    (st_xmax(st_transform(st_setsrid(proj.geom,proj.likely_srid),3006)) - st_xmin(st_transform(st_setsrid(proj.geom,proj.likely_srid),3006))) as x_dispersion,
    (st_ymax(st_transform(st_setsrid(proj.geom,proj.likely_srid),3006)) - st_ymin(st_transform(st_setsrid(proj.geom,proj.likely_srid),3006))) as y_dispersion,
		proj.likely_srid
    from proj),

socken as (
    SELECT *
    FROM   dblink('dbname=urdar_reference_data','SELECT namn, lan, geom FROM socknar_sverige')
    AS     socknar_sverige(namn text, lan text, geom "public"."geometry")),

eezone as (
    SELECT *
    FROM   dblink('dbname=urdar_reference_data','SELECT eez_name, geom FROM eez_sweden')
    AS     eezone(eez_name text, geom "public"."geometry")),
		
count_project as (
    select 1 as rownum,
    count("ClassId") as project_count from "Object" where "ClassId" = 1),

site_info as (
    SELECT
        1 as rownum,
        "Object"."Name" as site_name, 
        array_agg("Attribute"."Label" || ': ' || "AttributeValue"."Value") as site_attributes,
        current_database() as site_archive
    FROM
        "Object"
        INNER JOIN
        "Attribute"
        ON 
        "Object"."ObjectId" = "Attribute"."ObjectId"
        INNER JOIN
        "AttributeValue"
        ON 
            "Attribute"."AttributeId" = "AttributeValue"."AttributeId"
    where "ClassId" = 1
    GROUP BY 	
    "Object"."ObjectId",current_database(),
        "Object"."Name"
    ),
    
atts as (
        SELECT 1 as rownum,
        count("Label") FILTER(WHERE "MetaId" = 131) as sample_count,
        count("Label") FILTER(WHERE "MetaId" = 119) as find_count
    FROM
        "Attribute"),
        
        objs as ( SELECT 
        1 as rownum,
        count("ClassId") FILTER(WHERE "ClassId" = 11) as surveyed_arch_object_count,
        count("ClassId") FILTER(WHERE "ClassId" = 2) as fornlamning_count,
        count("ClassId") FILTER(WHERE "ClassId" = 9) as omrade_or_schakt_count
    FROM "Object"),

startend as (
    SELECT 1 as rownum, 
    (select min(to_char("Time", 'YYYY-MM')) FROM "Event") as db_creation_date, 
    (select min(to_char("Time",'YYYY-MM')) as end_of_fieldwork FROM "Event" where "Description" LIKE 'Import from Measurement File%') as start_of_fieldwork,
    (select max(to_char("Time",'YYYY-MM')) as end_of_fieldwork FROM "Event" where "Description" LIKE 'Import from Measurement File%') as end_of_fieldwork
    ),

db_info as(
    select 1 as rownum,array_accum(table_name) AS table_array,count(*) as table_count 
    FROM (select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' order by table_name) tables),
		
spatial_types as (
select
1 as rownum,
count(b."Type") FILTER(WHERE b."MetaId" in (38,39)) as point_count,
count(b."Type") FILTER(WHERE b."MetaId" = 80) as polygon_count,
count(b."Type") FILTER(WHERE b."MetaId" = 76) as polyline_count
from "GeoObject" a inner join "GeoObjectDef" b on a."MetaId" = b."MetaId"
	)

select 
d.site_name,
i.table_array,
i.table_count,
g.db_creation_date,
g.start_of_fieldwork,
g.end_of_fieldwork,
d.site_archive,
c.project_count,
e.omrade_or_schakt_count,
e.surveyed_arch_object_count,
e.fornlamning_count,
f.sample_count,
f.find_count,
j.point_count,
j.polygon_count,
j.polyline_count,
b.namn as parish_name,
b.lan as county_name,
a.site_centroid_wkt,
a.x_dispersion,
a.y_dispersion,
(SELECT "AttributeValue"."Value" FROM "Attribute" INNER JOIN "AttributeValue" ON "Attribute"."AttributeId" = "AttributeValue"."AttributeId" where "Attribute"."MetaId" = 489 LIMIT 1)::integer as intrasis_srid,
a.likely_srid,
d.site_attributes,
a.site_centroid_wkb,
h.eez_name
    
from cent a
    left join socken b on ST_Intersects(a.site_centroid_wkb, b.geom)
    right join count_project c on a.rownum = c.rownum
    right join site_info d on a.rownum = d.rownum
    right join objs e on a.rownum = e.rownum	
    right join atts f on a.rownum = f.rownum
		right join spatial_types j on a.rownum = j.rownum
    right join startend g on a.rownum = g.rownum
    left join eezone h on ST_Intersects(a.site_centroid_wkb, h.geom)
    inner join db_info i on a.rownum = i.rownum;
 end;
$func$
LANGUAGE plpgsql;
"""

array_accum_func = """
CREATE OR REPLACE AGGREGATE array_accum (anyelement)
    (sfunc = array_append,stype = anyarray,initcond = '{}');
"""
example_array_accum_use = """
select 1 as rownum,array_accum(table_name)::text AS table_array,count(*)::bigint as table_count 
FROM (select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' order by table_name) tables;
"""


create_attribute_value_table = """
create table all_attribute_values (
pk serial primary key,
value varchar unique);
"""

populate_geo_obj_attr = """
with base as (
select * 
from "GeoObject" as a 
inner join "GeoRel" as b on a."ObjectId" = b."GeoObjectId"
inner join "Object" as c on b."ObjectId" = c."ObjectId"
inner join "Attribute" as d on d."ObjectId" = c."ObjectId"
inner join "AttributeValue" as e on e."AttributeId" = d."AttributeId"
)
INSERT INTO all_geo_object_attribute_values
select "Value" from base 
ON CONFLICT DO NOTHING
"""

update_value_extraction_status = r"""
update all_attribute_values set extraction_status = case
when value ~* '(?<!fyllning[ | i ]+)(vägg|dubbel)?[st]+.?.?s?[to]+.?.?t?[ol]+.?.?o?[lp]+.?.?l?[ph]+.?.?p?[hå]+.?.?h?[ål]+.?.?å?l+.?.?l?\??(?!s?fyll+)'
then '1. value used to define posthole features' 
else '0. value not used' 
end
"""
