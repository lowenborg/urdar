--CREATE OR REPLACE VIEW view_geo_objects_attribute_arrays AS

WITH attribute_data AS
(SELECT "Object"."ObjectId",
    "Object"."PublicId",
    "Object"."Name",
    ARRAY_AGG(view_object_attributes."Label" || ': ' || (CASE WHEN view_object_attributes."Value" LIKE '' THEN 'no data' ELSE view_object_attributes."Value" END)) AS attribute_info
   FROM "Object"
   LEFT JOIN view_object_attributes ON "Object"."ObjectId" = view_object_attributes."ObjectId"
	 GROUP BY "Object"."ObjectId",
    "Object"."PublicId",
    "Object"."Name"
		),


object_definitions AS 
(SELECT "ObjectDef"."PublicId",
		"ObjectDef"."Code",
		"Definition"."Name" as definition,
		"Definition"."Description" as definition_description,
		"Definition"."Help" as definition_help
		FROM "ObjectDef" 
		INNER JOIN "Definition" ON "ObjectDef"."MetaId" = "Definition"."MetaId"
		),

object_relations as 
(SELECT "ObjectRel"."ChildId","ObjectRel"."ParentId","Definition"."Name" as relationship_type
		FROM "ObjectRel" 
		INNER JOIN "Definition" ON "ObjectRel"."MetaId" = "Definition"."MetaId"
		),
		
parent_relationships as 
(SELECT "ChildId", ARRAY_AGG('parent: ' || "ParentId" || ', relationship type: ' || relationship_type) as parent_relations FROM object_relations group by "ChildId"),


child_relationships as 
(SELECT "ParentId", ARRAY_AGG('child: ' || "ChildId" || ', relationship type: ' || relationship_type) as child_relations FROM object_relations group by "ParentId")

SELECT
	ROW_NUMBER() OVER (ORDER BY "Object"."ObjectId") AS row_number,
	"Object"."ObjectId", 
	"GeoObject"."ObjectId" as geo_object_id,	
	"Object"."ClassId", 
	"Object"."SubClassId", 
	"Object"."PublicId",
	"Object"."Name",
	attribute_data.attribute_info,
	object_definitions."Code",
	ARRAY_AGG(object_definitions.definition || ', description: ' || (CASE WHEN object_definitions.definition_description LIKE '' then 'no description' ELSE object_definitions.definition_description end) 
	|| ', help: ' || CASE WHEN object_definitions.definition_help LIKE '' then 'no info' ELSE object_definitions.definition_help end) as object_definition,
	parent_relationships.parent_relations,
	child_relationships.child_relations,
	"GeoObject".the_geom	

FROM
	"Object"
	INNER JOIN
	"GeoRel"
	ON 
		"Object"."ObjectId" = "GeoRel"."ObjectId"
	INNER JOIN
	"GeoObject"
	ON 
		"GeoRel"."GeoObjectId" = "GeoObject"."ObjectId"
	LEFT JOIN
		attribute_data
	ON
	"Object"."ObjectId" = attribute_data."ObjectId"
	LEFT JOIN
		object_definitions
	ON
	"Object"."PublicId" = object_definitions."PublicId"
	LEFT JOIN
	parent_relationships 
	ON
	"Object"."ObjectId" = parent_relationships."ChildId"
	LEFT JOIN
	child_relationships
	ON
	"Object"."ObjectId" = child_relationships."ParentId"
GROUP BY 
	"Object"."ObjectId", 
	"GeoObject"."ObjectId",	
	"Object"."ClassId", 
	"Object"."SubClassId", 
	"Object"."PublicId",
	"Object"."Name",
	attribute_data.attribute_info,
	object_definitions."Code",
	parent_relationships.parent_relations,
	child_relationships.child_relations,
	"GeoObject".the_geom	