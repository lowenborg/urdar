# ---------------------------------------------
# Various functions for automation
# ---------------------------------------------
# Install dependencies
import pandas as pd
from urdar_dev import backend as db

######################
# Automation functions
######################
def intrasis_site_data(dbname, password):
    """
    Uppsala University 2020
    Description: Extracts site info from a database or a list of databases.
    @author: gisli.palsson@gmail.com
    """
    # connect to the collection server
    connection, eng = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = connection.cursor()
    # extract site attributes
    print(f"Scanning database {dbname}")
    cursor.execute(
        f"SELECT site_archive from intrasis_archives where site_archive like '{dbname}'"
    )
    if cursor.rowcount == 1:
        print(f"{dbname} has already been processed")
    else:
        sql_command = f"""
        INSERT INTO intrasis_archives(site_name,table_array,table_ct,db_creation_date,start_of_fieldwork,end_of_fieldwork,site_archive,project_ct,omrade_schakt_ct,arch_obj_ct,fornlamn_ct,sample_ct,find_ct,point_ct,polygon_ct,polyline_ct,parish_name,lan_name,geom_centroid_wkt,x_dispersion,y_dispersion,intrasis_srid,likely_srid,site_attributes,geom,eez_name)  
        SELECT * FROM dblink('dbname={dbname}', 'select (site_attributes()).*') 
                AS t(
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
                expected_srid	int4,
                likely_srid int4,
                site_attributes text[],
                geom_centroid "public"."geometry",
                eez_name text
                )
                ON CONFLICT (site_archive) DO NOTHING"""
        cursor.execute(sql_command)
    # closes the connection to the database
    cursor.close()


# site srid update function
def update_srid(password):
    connection, engine = db.urdar_login("urdar_overseer", password)
    intrasis_databases = pd.read_sql(
        "SELECT site_archive,likely_srid FROM intrasis_archives order by site_archive",
        connection,
    )
    for j, i in intrasis_databases.iterrows():
        # connect to target server
        connection, engine = db.urdar_login(i[0], password)
        # create a cursor object
        cursor = connection.cursor()
        # print database and srids
        print(f"Reprojecting geometries in {i[0]} from {i[1]} to 3006")
        cursor.execute(
            f"""
        update "GeoObject" set the_geom = st_transform(st_setsrid(the_geom,{i[1]}),3006)
        """
        )
        # close the connection to the database
        cursor.close()

# function to load custom functions
def function_loader(dbname, password):
    """function preloader"""
    # connect to the target server
    connection, engine = db.urdar_login(dbname, password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # create an array function to enable the aggregation of all table names into an array
    cursor.execute(db.array_accum_func)
    # extract site attributes
    cursor.execute(db.site_attr_func)
    # closes the connection to the database
    cursor.close()

# populate urdar table
def populate_overseer_table(insert, dbname, password):
    con, eng = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = con.cursor()
    print(f"Scanning database {dbname}")
    # execute and close the connection
    cursor.execute(insert)
    cursor.close()

# counting finds
def find_count(dbname, password):
    connection, engine = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # query the database
    insert_ct = f"""
    INSERT INTO find_count(intrasis_archive,find_type,find_count)
    SELECT * FROM dblink('dbname={dbname}',
    'select ''{dbname}'' as intrasis_archive,attrval."Value", count (*) as ct
				from "Object" obj left join "Attribute" attr on obj."ObjectId" = attr."ObjectId" 
                left join "AttributeValue" attrval on attr."AttributeId" = attrval."AttributeId"
				where attr."MetaId" = 122 group by attrval."Value"') 
                AS t(
                intrasis_archive	varchar,
                find_type text,
                find_count	int4
                )
                ON CONFLICT (intrasis_archive,find_type) DO NOTHING"""
    cursor.execute(insert_ct)
    cursor.close()

# counting geometries per lan
def geo_in_lan(dbname, password):
    connection, engine = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # query the database
    insert = f"""
    INSERT INTO geo_objs_in_lan(intrasis_archive,lan,objects_in_lan,object_ids,count_rank)
    SELECT * FROM dblink('dbname={dbname}',
    'with
    socken as (
        SELECT *
        FROM   dblink(''dbname=urdar_reference_data'',''SELECT namn, lan, geom FROM socknar_sverige'')
        AS     socknar_sverige(namn text, lan text, geom "public"."geometry")),
    counts as (		
    select ''{dbname}'' as intrasis_archive,b.lan,count(*) as objects_in_lan,array_agg(a."ObjectId") as object_ids
    from "GeoObject" a
        left join socken b on ST_Intersects(a.the_geom, b.geom)
        group by b.lan)
    select *,row_number() over (order by objects_in_lan desc) as count_rank from counts') 
                    AS t(
                    intrasis_archive	varchar,
                    lan text,
                    objects_in_lan int8,
                    object_ids _int4,
                    count_rank int8
                    )
    ON CONFLICT (intrasis_archive,lan) DO NOTHING"""
    try:
        cursor.execute(insert)
    except Exception as e:
        cursor.execute(
            f"insert into geo_objs_in_lan(intrasis_archive,lan,error_message) values ('{dbname}','Error in parsing geometry','{e}')"
        )
    cursor.close()


def all_attrvalues(dbname, password):
    connection, engine = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # query the database
    insert_ct = f"""
    INSERT INTO all_attribute_values(value)
    SELECT * FROM dblink('dbname={dbname}',
    'select distinct "Value" from "AttributeValue"') 
                AS t(
                value varchar)
                ON CONFLICT (value) DO NOTHING"""
    cursor.execute(insert_ct)
    cursor.close()


def geo_obj_attrvals(dbname, password):
    connection, engine = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # query the database
    insert_ct = f"""
    INSERT INTO all_geo_object_attribute_values(value)
    SELECT * FROM dblink('dbname={dbname}',
    'with base as (
    select * 
    from "GeoObject" as a 
    inner join "GeoRel" as b on a."ObjectId" = b."GeoObjectId"
    inner join "Object" as c on b."ObjectId" = c."ObjectId"
    inner join "Attribute" as d on d."ObjectId" = c."ObjectId"
    inner join "AttributeValue" as e on e."AttributeId" = d."AttributeId")
    select distinct "Value" from base') 
                AS t(
                value varchar)
                ON CONFLICT (value) DO NOTHING"""
    cursor.execute(insert_ct)
    cursor.close()


# counting postholes
def count_features(dbname, password):
    connection, engine = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # query the database
    insert_ct = f"""
    INSERT INTO feature_count(intrasis_archive,feature,feature_count,feature_proportion,total_features)
    SELECT * FROM dblink('dbname={dbname}',
    'with base as (
    select * 
    from "GeoObject" as a 
    inner join "GeoRel" as b on a."ObjectId" = b."GeoObjectId"
    inner join "Object" as c on b."ObjectId" = c."ObjectId"
    inner join "Attribute" as d on d."ObjectId" = c."ObjectId"
    inner join "AttributeValue" as e on e."AttributeId" = d."AttributeId"
    )
    select ''{dbname}'' as intrasis_archive,''0. all postholes'' as "Value",count(*) as value_occurrence, round((count(*)::float/(select case when count(*) > 0 then count(*) else 1 end from base))::numeric,2) as value_proportion,(select count(*) from base) as total_features from base where "Value" ~* ''(?<!fyllning[ | i ]+)(vägg|dubbel)?[st]+.?.?s?[to]+.?.?t?[ol]+.?.?o?[lp]+.?.?l?[ph]+.?.?p?[hå]+.?.?h?[ål]+.?.?å?l+l?\??(?!s?fyllning+)''
    union all
    select ''{dbname}'' as intrasis_archive,"Value",count (*) as value_occurrence, round((count(*)::float/(select case when count(*) > 0 then count(*) else 1 end from base))::numeric,2) as value_proportion, (select count(*) from base) as total_features from base where "Value" ~* ''(?<!fyllning[ | i ]+)(vägg|dubbel)?[st]+.?.?s?[to]+.?.?t?[ol]+.?.?o?[lp]+.?.?l?[ph]+.?.?p?[hå]+.?.?h?[ål]+.?.?å?l+l?\??(?!s?fyllning+)''
    group by "Value"')
        as t(
            intrasis_archive varchar,
            feature varchar,
            feature_count int4,
            feature_proportion float4,
            total_features int8)
            ON CONFLICT (intrasis_archive,feature) DO NOTHING"""
    cursor.execute(insert_ct)
    cursor.close()


# counting edges anod nodes
def count_edge_nodes(dbname, password):
    connection, engine = db.urdar_login("urdar_overseer", password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # query the database
    insert_ct = f"""
    INSERT INTO network.node_edge_count(intrasis_archive,node_count,edge_count)
    SELECT * FROM dblink('dbname={dbname}',
    'select ''{dbname}'' as intrasis_archive, count(*) as nodes, (select count(*) from "ObjectRel") as edges from "GeoObject" where the_geom is not null')
    as t(intrasis_archive varchar,node_count int4, edge_count int4)
    ON CONFLICT (intrasis_archive) DO NOTHING"""
    cursor.execute(insert_ct)
    cursor.close()


# the site data exctraction function.
def old_intrasis_site_data(dbname, password):
    """
    Uppsala University 2020
    Description: The old version of the site extractor. Just here for posterity
    @author: gisli.palsson@gmail.com
    """
    # connect to the server
    connection, engine = db.urdar_login(dbname, password)
    # Create a cursor object
    cursor = connection.cursor()
    print(f"Scanning database {dbname}")
    # Find any 'site' in the Intrasis database. This is part of the validation process whereby dabatabases with multiple sites are flagged
    site = pd.read_sql(
        r'select "ObjectId" from "Attribute" where "MetaId" = 69', connection
    )
    if site.shape[0] > 1:
        print(f"More than one project detected in database {dbname}")
    else:
        print(f"One project detected in database {dbname}")
    # identify site SRID:
    srid = pd.read_sql(
        """SELECT "AttributeValue"."Value" FROM "Attribute" INNER JOIN "AttributeValue" 
    ON "Attribute"."AttributeId" = "AttributeValue"."AttributeId" where "Attribute"."MetaId" = 489""",
        connection,
    )
    srid["FixedSrid"] = srid["Value"].replace({"0": "3006"})
    # create an array function to enable the aggregation of all table names into an array
    cursor.execute(db.array_accum_func)
    # extract site attributes
    sql_command = "with b as (select st_transform(st_setsrid(st_centroid(the_geom), "
    try:
        sql_command += str(int(srid.iloc[0]["FixedSrid"]))
    except:
        sql_command += "3006"
    sql_command += """),3006) as geom from "GeoObject"),
    cent as (
    select 
    1 as rownum,
    ST_GeometricMedian(st_union(geom)) as site_centroid_wkb,
    ST_AsText(ST_GeometricMedian(st_union(geom))) as site_centroid_wkt,
    (st_xmax(st_union(geom)) - st_xmin(st_union(geom))) as x_dispersion,
    (st_ymax(st_union(geom)) - st_ymin(st_union(geom))) as y_dispersion
    from b),

    socken as (
    SELECT *
    FROM   dblink('dbname=urdar_reference_data','SELECT namn, lan, geom FROM socknar_sverige')
    AS     socknar_sverige(namn text, lan text, geom "public"."geometry")),

    count_project as (select
    1 as rownum,
    count("ClassId") as project_count from "Object" where "ClassId" = 1),

    site_info as 
    (SELECT
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
    
    atts as (SELECT 
        1 as rownum,
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

select 1 as rownum,array_accum(table_name) AS table_array,count(*) as table_count 
FROM (select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' order by table_name) tables

insert into 

dblink('dbname=urdar_overseer','SELECT namn, lan, geom FROM socknar_sverige')
    AS     socknar_sverige(namn text, lan text, geom "public"."geometry")),

intrasis_archives
(site_name,table_array,table_ct,db_creation_date,start_of_fieldwork,end_of_fieldwork,site_archive,project_ct,
omrade_schakt_ct,arch_obj_ct,fornlamn_ct,sample_ct,find_ct,parish_name,geom_centroid_wkt,x_dispersion,y_dispersion,site_srid,site_attributes,geom_centroid)


    select 
    site_name,
    table_array,
	table_count,
    db_creation_date,
	start_of_fieldwork,
    end_of_fieldwork,
    site_archive,
    project_count,
    omrade_or_schakt_count,
    surveyed_arch_object_count,
    fornlamning_count,
    sample_count,
    find_count,
    namn as parish_name,
    lan as county_name,
    site_centroid_wkt,
    x_dispersion,
    y_dispersion,
    site_attributes,
    """
    try:
        sql_command += str(int(srid.iloc[0]["Value"]))
    except:
        sql_command += "0"
    sql_command += """ as site_srid,
    site_centroid_wkb
    from cent 
        left join socken on ST_Intersects(cent.site_centroid_wkb, socken.geom)
        right join count_project on cent.rownum = count_project.rownum
        right join site_info on cent.rownum = site_info.rownum
        right join objs on cent.rownum = objs.rownum	
        right join atts on cent.rownum = atts.rownum
        right join startend on cent.rownum = startend.rownum
    	inner join db_info on cent.rownum = db_info.rownum
    ON CONFLICT (site_archive) DO NOTHING    
    """
    site_attributes = pd.read_sql(sql_command, connection)

    # closes the connection to the database
    cursor.close()