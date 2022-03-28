create or replace view view_object_attributes as 

with attribute_metadata as (

SELECT
	"Attribute"."AttributeId", 
	"Attribute"."MetaId" as attribute_metaid, 
	"Attribute"."ObjectId", 
	"Attribute"."Label", 
	"AttributeDef"."MultipleAllowed", 
	"AttributeDef"."FreeValueAllowed", 
	"AttributeDef"."NotNull", 
	"AttributeDef"."DataType", 
	"AttributeDef"."Unit", 
	"AttributeDef"."Length"
FROM
	"Attribute"
	INNER JOIN
	"AttributeDef"
	ON 
		"Attribute"."MetaId" = "AttributeDef"."MetaId"
		
		)

select 
	"AttributeValue"."AttributeId",
	"AttributeValue"."AttributeValueId",
	"AttributeValue"."Unit",
	"AttributeValue"."Value",
	attribute_metadata.attribute_metaid,
	attribute_metadata."ObjectId",
	attribute_metadata."Label",
	attribute_metadata."MultipleAllowed",
	attribute_metadata."FreeValueAllowed",
	attribute_metadata."NotNull", 
	attribute_metadata."DataType", 
	attribute_metadata."Length"

from "AttributeValue" left join attribute_metadata on "AttributeValue"."AttributeId" = attribute_metadata."AttributeId"

