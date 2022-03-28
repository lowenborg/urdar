"""
Just a collection of sql commands used globally in the project
"""

#####################################
# Select all databases on the server
#####################################
select_dbs="""
SELECT datname FROM pg_database order by datname
"""

######################################
# Create geometry field
######################################
table = 'set table name'
field = 'set field name'
geom_type = 'set geom type'
srid = 'set srid'

add_geom_field = f"""
ALTER TABLE {table} add {field} geometry({geom_type},{srid});

CREATE INDEX {table}_geom_idx
  ON public.{table}  
	USING GIST ({field});
"""    


####################################
# field generator for an empty table
####################################
add_fields ="""
alter table eez_sweden add pk serial primary key;
alter table eez_sweden add eez_name varchar(250);
alter table eez_sweden add geom geometry(Multipolygon,3006);
CREATE INDEX eez_geom_idx
  ON public.eez_sweden  
	USING GIST (geom);
"""



#######################################
# Drafts of specific object extractions
#######################################
select_finds ="""
select 
obj."ObjectId",
obj."ClassId",
obj."SubClassId",
obj."PublicId",
obj."Name",
obj."SiteId",
obj."Origin",
attr."AttributeId",
attr."MetaId",
attr."Label",
attrval."AttributeValueId",
attrval."Value"

from "Object" obj left join "Attribute" attr on obj."ObjectId" = attr."ObjectId" left join "AttributeValue" attrval on attr."AttributeId" = attrval."AttributeId"
where  attr."MetaId" = 122
"""

count_finds =f"""
select ''{dbname}'' as intrasis_archive,attrval."Value", count (*) as ct
from "Object" obj left join "Attribute" attr on obj."ObjectId" = attr."ObjectId" left join "AttributeValue" attrval on attr."AttributeId" = attrval."AttributeId"
where attr."MetaId" = 122 group by attrval."Value"'
group by attrval."Value"
"""

# just update with variable for table, index name and indexed fields.
unique_index ="""
create unique index find_type on find_count(intrasis_archive,find_type);
"""		



# postholes
prototype_count_queries=r"""
select "Value",count(*) as value_occurrence from "AttributeValue" group by "Value" order by "Value";

select "Value",count(*) as value_occurrence from "AttributeValue" where "Value" ilike '%t_lp%' group by "Value" order by "Value";

select "Value",count("Value") from "AttributeValue" where "Value" ~* 'Stolph+p?ål\??(?!s?fyll+)' group by "Value";

select "Value",count("Value") from "AttributeValue" where "Value" ~* '(?<!fyllning[ | i ]+)(vägg|dubbel)?stolph+p?ål\??(?!s?fyll+)' group by "Value";

select "Value",count(*) as value_occurrence from "AttributeValue" where "Value" ~* '[st]+.?s?[to]+.?t?[ol]+.?o?[lp]+.?l?[ph]+.?p?[hå]+.?h?[ål]+.?å?l+.?l?' group by "Value" order by "Value";

select "Value",count(*) as value_occurrence from "AttributeValue" where "Value" ilike '%st_lp%' group by "Value"
except
select "Value",count(*) as value_occurrence from "AttributeValue" where "Value" ~* '[st]+.?s?[to]+.?t?[ol]+.?o?[lp]+.?l?[ph]+.?p?[hå]+.?h?[ål]+.?å?l+.?l?' group by "Value" order by "Value";
"""


create_feature_count_table  = """
create table posthole_count (
pk serial primary key,
intrasis_archive varchar(255),
feature varchar(255),
feature_count int4,
feature_proportion float4
);

create unique index on feature_count (intrasis_archive,feature);
"""

create_node_edge_table = """
create table node_edge_count (
pk serial primary key,
intrasis_archive varchar unique,
node_count int4,
edge_count int4
)
"""

levenshtein_distance_calc = """
with base as (
select pk, word, levenshtein(upper(word), 'STOLPHÅL') 
  from all_geo_object_attribute_values
       cross join lateral regexp_split_to_table(value, '\y') r(word) 
 where length(word) > 3
   and levenshtein(upper(word), 'STOLPHÅL') < 11),

mi as (select min(levenshtein) as levenshtein, pk from base group by pk),

synth as (
select mi.pk,mi.levenshtein,base.word from mi inner join base on mi.pk = base.pk and mi.levenshtein = base.levenshtein),

dist as (
select distinct on (pk) * from synth)

update all_geo_object_attribute_values set levenshtein_stlp_distance = dist.levenshtein from dist
where all_geo_object_attribute_values.pk = dist.pk
;
"""

posthole_attributes = """
--this is more extensive than it needs to be for this purpose as it is fairly extensible and applicable for future uses
with attrs as (
select 
aa."AttributeId" as attribute_id,
aa."MetaId" as attribute_meta_id,
aa."ObjectId" as object_id,
aa."Label" as attribute_label,
ab."AttributeValueId" as attributevalue_id,
ab."Value" || case when ab."Unit" is not null or ab."Unit" not like '' then ' ' || ab."Unit" else null end as attributevalue_value,
ag."Name" as definition_name,
ag."Description" as definition_description,
ag."VersionReferenceId" as definition_versionreference,
ah."Value" as alternative_def_value,
aa."Label" || ': ' || (case when ab."Value" is null or ab."Value" like '' then '<null>' else ab."Value" end) || case when ab."Unit" is not null then ' ' || ab."Unit" else null end as attributevalue
from "Attribute" aa
inner join "AttributeValue" ab on aa."AttributeId" = ab."AttributeId"
inner join "AttributeDef" ac on aa."MetaId" = ac."MetaId"
left join "FreeText" af on aa."AttributeId" = af."AttributeId"
left join "Definition" ag on ac."MetaId" = ag."MetaId"
left join "AlternativeDef" ah on ac."MetaId" = ah."MetaId"
)

select object_id, array_agg(attributevalue) as attr_array from attrs group by object_id
order by object_id
"""

posthole_investigations = """
-- shows that MetaId 10047 maps to AttributeDefId 10042, Value 'Stolphål'
SELECT * FROM "AlternativeDef" where "MetaId" in (183,10047,10042);

-- picks up the 'type' attribute, which I presume is a general attribute for interpretations
SELECT * FROM "Attribute" where "MetaId" in (183,10047,10042);

-- more metadata on attribute 10042 'Typ'
SELECT * FROM "AttributeDef" where "MetaId" in (183,10047,10042);

-- shows the expected definitions for the three IDs
SELECT * FROM "Definition" where "MetaId" in (183,10047,10042);

-- shows some data about the posthole object class
SELECT * FROM "ObjectDef" where "MetaId" in (183,10047,10042);

-- shows relationship between MetaId 183 and ClassId 11
SELECT * FROM "SubClassDef" where "MetaId" in (183,10047,10042);
"""
srid_intersect = """
with socken as (
    SELECT *
    FROM   dblink('dbname=urdar_reference_data','SELECT geom FROM socknar_sverige')
    AS     socknar_sverige(geom "public"."geometry"))

select 
case 
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3006)) = 3006 then 3006
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3007)) = 3007 then 3007
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3008)) = 3008 then 3008
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3009)) = 3009 then 3009
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3010)) = 3010 then 3010
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3015)) = 3015 then 3015
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3018)) = 3018 then 3018
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3019)) = 3019 then 3019
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3020)) = 3020 then 3020
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3021)) = 3021 then 3021
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3022)) = 3022 then 3022
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3025)) = 3025 then 3025
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3026)) = 3026 then 3026
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3027)) = 3027 then 3027
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),3298)) = 3298 then 3298
when st_srid(st_transform(st_centroid(st_collect(a.the_geom)),4124)) = 4124 then 4124
else null end
from "GeoObject" a 
inner join socken b on ST_Intersects(a.the_geom, b.geom);
"""

detect_intersects = """
insert into geo_objs_intersects

with selection as (select databases,db_occurrence,the_geom,pk from geo_objs_groupby_geom)

select a.*,array_agg(b.pk) as intersect_pk,array_agg(array_to_string(b.databases,',')) as intersect_dbs
from selection a left join geo_objs_groupby_geom b on st_intersects(a.the_geom,b.the_geom)
where a.pk != b.pk
group by a.databases,a.db_occurrence,a.the_geom,a.pk
"""

# rtf function conversion.
convert_rtf =
"""
with replacements as (
select *, replace(replace(replace(
"Text",
'\''e4','ä'),
'\''f6','ö'),
'\''e5','å')
as rep_test,
    strpos("Text",(regexp_matches("Text",'\w{2}\d{2} \w'))[1])+5 as startpos,
    case when "AttributeId" > 100000 then strpos("Text",(regexp_matches("Text",'\{\n\{'))[1])
else strpos("Text",(regexp_matches("Text",'\\cf0'))[1]) end as endpos

from "FreeText"),

iterate as (
select "AttributeId",
"Text",
regexp_replace(regexp_replace(regexp_replace(regexp_replace(replace(
substring(rep_test,startpos,(endpos-startpos)),
'\par',''),
'\/\w+\\\w+',''),
'\n[ ]*\}',''),
'\\\w{2}\d+',''),
'\\\w+','')
as converted_text from replacements)


select "AttributeId",
"Text",
regexp_replace(regexp_replace(regexp_replace(regexp_replace(
converted_text,
'\\\w{2}\d+',''),
'[\n ]{2,}',''),
'[\n]{2,}',''),
' /\w\\\w', '')
as converted_text from iterate
"""
create_unnesting_table= """
create table intrasis_archives_unnested_attributes as 
with unnesting as (
select site_archive,unnest(site_attributes) as attributes from intrasis_archives
),

parsed as (
select 
site_archive,
case when attributes like 'End date%' or attributes like 'Slutdatum%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as slutdatum,
case when attributes like 'Type of excavation%' or attributes like '"Undersökningstyp%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as undersokningstyp,
case when attributes like 'Type of exploatation%' or attributes like 'Exploateringstyp%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as exploateringstyp,
case when attributes like 'Projektnamn%' or attributes like 'Project name%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as projektnamn,
case when attributes like 'Projektkod%' or attributes like 'Project code%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as projektkod,
case when attributes like 'Raä Dnr%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as raa_dnr,
case when attributes like 'Lst Dnr%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as lst_dnr,
case when attributes like 'Plats%' or attributes like 'Place%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as plats,
case when attributes like 'Site Id%' or attributes like 'Undersöknings Id%' then case when length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1 then array_to_string(regexp_match(attributes, ': (.*)'),',') else null end end as undersöknings_id

from unnesting
where length(array_to_string(regexp_match(attributes, ': (.*)'),',')) > 1
)

select 

site_archive,
max(slutdatum) as slutdatum,
max(undersokningstyp) as undersokningstyp,
max(exploateringstyp) as exploateringstyp,
max(projektnamn) as projektnamn,
max(projektkod) as projektkod,
max(raa_dnr) as raa_dnr,
max(lst_dnr) as lst_dnr,
max(plats) as plats,
max(undersöknings_id) as undersöknings_id

from parsed

group by site_archive
order by site_archive
"""



#####
# Insert fixed geometries
#####
fix_geoms = """
with geoobjs as (
select geom,
unnest(string_to_array(object_ids,',')) as object_id from geo_objs_all
)

update "GeoObject" set the_geom = b.geom from geoobjs b where "GeoObject"."ObjectId" = b.object_id::int;


with geoobjs as (
select geom,
unnest(string_to_array(object_ids,',')) as object_id from geo_objs_all_line
)

update "GeoObject" set the_geom = b.geom from geoobjs b where "GeoObject"."ObjectId" = b.object_id::int;



with geoobjs as (
select geom,
unnest(string_to_array(object_ids,',')) as object_id from geo_objs_all_points
)

update "GeoObject" set the_geom = b.geom from geoobjs b where "GeoObject"."ObjectId" = b.object_id::int;
"""