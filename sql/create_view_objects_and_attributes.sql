CREATE OR REPLACE VIEW view_objects_and_attributes as

SELECT
	"Object"."ObjectId", 
	"Object"."ClassId", 
	"Object"."SubClassId", 
	"Object"."PublicId", 
	"Object"."Name", 
	"Object"."SiteId", 
	"Object"."Origin", 
	view_object_attributes."AttributeId", 
	view_object_attributes."AttributeValueId", 
	view_object_attributes."Unit", 
	view_object_attributes."Value", 
	view_object_attributes.attribute_metaid, 
	view_object_attributes."Label", 
	view_object_attributes."DataType", 
	view_object_attributes."Length"
FROM
	"Object"
	LEFT JOIN
	view_object_attributes
	ON 
		"Object"."ObjectId" = view_object_attributes."ObjectId"