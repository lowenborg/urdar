import geopandas as gpd
import pandas as pd
from urdar_dev import backend as db
import fiona
from pathlib import Path
import os
from decouple import config

###########
# Login
###########
pg_dbname = 'urdar_overseer'
password = config('PASS')
con, engine = db.urdar_login(pg_dbname,password)

# find which gpkgs have already been made
full_path = Path("D:/Dropbox/palsson_analytics/urdar/python_code/output/csv")
dfdbases = pd.read_sql('select site_archive from public.intrasis_archive_tasks',con)
dbases_all = dfdbases['site_archive'].tolist()

completed_exports = []
with os.scandir(full_path) as folder:
    for entry in folder:
        if entry.name.endswith(".csv") and entry.is_file():
            completed_exports.append(entry.name[:-4])

dbases_toprocess =  [x for x in dbases_all if x not in completed_exports] 

for dbase in dbases_toprocess:
  # create csv
  table = pd.read_sql(fr"""
  with objs as (
      SELECT distinct on (a."ObjectId")
      a."ObjectId" as object_id,
      a."PublicId" as object_public_id,
      a."ClassId" as object_class_id,
      a."SubClassId" as object_subclass_id,
      a."Name" as object_name,
      f."Name" as class,
      e."Name" as subclass,
      c."ObjectId" as geoobject_id,
      st_makevalid(c.the_geom) as geom,
      c."MetaId" as geoobject_meta_id,
      c."SymbolId" as geoobject_symbol_id,
      d."Type" as spatial_type
      from {dbase}."Object" a
      inner join {dbase}."GeoRel" b on a."ObjectId" = b."ObjectId"
      inner join {dbase}."GeoObject" c on b."GeoObjectId" = c."ObjectId"
      inner join {dbase}."GeoObjectDef" d on c."MetaId" = d."MetaId"
      inner join {dbase}."Definition" e on a."SubClassId" = e."MetaId"
      inner join {dbase}."Definition" f on a."ClassId" = f."MetaId"
      ),

      attrs as (
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
      aa."Label" || ': ' || (case when ab."Value" is null or ab."Value" like '' then '' else ab."Value" end) || case when ab."Unit" is not null then ' ' || ab."Unit" else null end as attributevalue
      from {dbase}."Attribute" aa
      inner join {dbase}."AttributeValue" ab on aa."AttributeId" = ab."AttributeId"
      inner join {dbase}."AttributeDef" ac on aa."MetaId" = ac."MetaId"
      left join {dbase}."FreeText" af on aa."AttributeId" = af."AttributeId"
      left join {dbase}."Definition" ag on ac."MetaId" = ag."MetaId"
      left join {dbase}."AlternativeDef" ah on ac."MetaId" = ah."MetaId"
      ),

      attrarray as (select object_id, array_agg(attributevalue) as attr_array from attrs group by object_id),
      parent_rels as (select "ParentId" as parent_id, array_agg("RelationDef"."ParentText" || ' ' || "ChildId") as parent_relations from {dbase}."ObjectRel" inner join {dbase}."RelationDef" on "ObjectRel"."MetaId" = "RelationDef"."MetaId" group by "ParentId"),
      child_rels as (select "ChildId" as child_id, array_agg("RelationDef"."ChildText" || ' ' || "ParentId") as child_relations from {dbase}."ObjectRel" inner join {dbase}."RelationDef" on "ObjectRel"."MetaId" = "RelationDef"."MetaId" group by "ChildId"),

      grp as (
      select a."ObjectId" as group_id,a."Name" as group_name, b."ChildId" as object_id 
      from {dbase}."Object" a
      left join {dbase}."ObjectRel" b on a."ObjectId" = b."ParentId"
      where a."SubClassId" in (52,10394,9906)
      ),

      replacements as (
      select *, replace(replace(replace(replace(replace(replace(
      array_to_string((regexp_match("Text",'(?<=\\fs17 ).*')),','),
      '\''e4','ä'),
      '\''f6','ö'),
      '\''e5','å'),
      '\''c5','Å'),
      '\''c4','Ä'),
      '\''d6','Ö')
      as rep_test
      from {dbase}."FreeText"),

      iterate as (
      select "AttributeId",
      "Text",
      regexp_replace(regexp_replace(regexp_replace(regexp_replace(replace(
      rep_test,
      '\par',''),
      '\/\w+\\\w+',''),
      '\n[ ]*\}}',''),
      '\\\w{{2}}\d+',''),
      '\\\w+','')
      as converted_text from replacements),

      descr as (
      select b."ObjectId" as object_id,
      regexp_replace(regexp_replace(regexp_replace(regexp_replace(
      a.converted_text,
      '\\\w{{2}}\d+',''),
      '[\n ]{{2,}}',''),
      '[\n]{{2,}}',''),
      ' /\w\\\w', '')
      as description from iterate a inner join {dbase}."Attribute" b on a."AttributeId" = b."AttributeId")

      select
      row_number() over (order by objs.object_id) as pk,
      site.site_name,
      site.site_landskap,
      site.lan_name,
      site.parish_name,
      site.geom_centroid_wkt as site_centroid,
      siteatt.slutdatum,
      siteatt.undersokningstyp,
      siteatt.exploateringstyp,
      siteatt.projektnamn,
      siteatt.projektkod,
      siteatt.raa_dnr,
      siteatt.lst_dnr,
      siteatt.plats,
      siteatt.undersöknings_id,
      '{dbase}' as intrasis_archive,
      objs.object_id,
      --objs.object_public_id,
      --objs.object_class_id,
      --objs.object_subclass_id,
      objs.object_name,
      objs.class,
      objs.subclass,
      --objs.geoobject_id,
      st_asewkt(objs.geom) as geom_wkt,
      objs.spatial_type,
      attrarray.attr_array,
      parent_rels.parent_relations,
      child_rels.child_relations,
      descr.description as feature_description,
      grp.group_name,
      group_descr.description as group_description

      from objs
      left join attrarray on objs.object_id = attrarray.object_id
      left join parent_rels on objs.object_id = parent_rels.parent_id
      left join child_rels on objs.object_id = child_rels.child_id
      left join descr on objs.object_id = descr.object_id
      left join public.geo_objs_groupby_geom geo_objs on objs.geom = geo_objs.the_geom
      left join intrasis_archives site on '{dbase}' = site.site_archive
      left join intrasis_archives_unnested_attributes siteatt on '{dbase}' = siteatt.site_archive
      left join grp on objs.object_id = grp.object_id
      left join descr group_descr on grp.group_id = group_descr.object_id
      """,con)
  table.to_csv('output/csv/{}.csv'.format(dbase),index=False)
  print(f'Exporting {dbase}')