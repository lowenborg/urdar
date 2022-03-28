# ---------------------------------------------
# A Python data model of the Urdar test 1 schema
# ---------------------------------------------
from sqlalchemy import (
    ARRAY,
    Boolean,
    CHAR,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

# ---------------------------------------------
# The following tables are Intrasis and PostGIS system tables that don't hold any pertinent archaeological data.
# ---------------------------------------------
class HighIdCounter(Base):
    __tablename__ = "HighIdCounter"

    Cnt = Column(Integer, primary_key=True)
    NoMeaning = Column(Integer)


class LowIdCounter(Base):
    __tablename__ = "LowIdCounter"

    Cnt = Column(Integer, primary_key=True)
    NoMeaning = Column(Integer)


class ReservedId(Base):
    __tablename__ = "ReservedIds"

    PublicId = Column(Integer, primary_key=True)
    UserId = Column(Integer, nullable=False)
    Reserved = Column(DateTime, nullable=False, server_default=text("now()"))


class SysDef(Base):
    __tablename__ = "SysDefs"

    SystemId = Column(Integer, primary_key=True)
    MetaId = Column(Integer)


class VersionReference(Base):
    __tablename__ = "VersionReference"

    VersionReferenceId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('versionreference_id_seq'::regclass)"),
    )
    Name = Column(String(50), nullable=False)
    Description = Column(String(100))
    Type = Column(String(20))


t_geography_columns = Table(
    "geography_columns",
    metadata,
    Column("f_table_catalog", String),
    Column("f_table_schema", String),
    Column("f_table_name", String),
    Column("f_geography_column", String),
    Column("coord_dimension", Integer),
    Column("srid", Integer),
    Column("type", Text),
)


t_geometry_columns = Table(
    "geometry_columns",
    metadata,
    Column("f_table_catalog", String(256)),
    Column("f_table_schema", String),
    Column("f_table_name", String),
    Column("f_geometry_column", String),
    Column("coord_dimension", Integer),
    Column("srid", Integer),
    Column("type", String(30)),
)


t_raster_columns = Table(
    "raster_columns",
    metadata,
    Column("r_table_catalog", String),
    Column("r_table_schema", String),
    Column("r_table_name", String),
    Column("r_raster_column", String),
    Column("srid", Integer),
    Column("scale_x", Float(53)),
    Column("scale_y", Float(53)),
    Column("blocksize_x", Integer),
    Column("blocksize_y", Integer),
    Column("same_alignment", Boolean),
    Column("regular_blocking", Boolean),
    Column("num_bands", Integer),
    Column("pixel_types", ARRAY(Text())),
    Column("nodata_values", ARRAY(Float(precision=53))),
    Column("out_db", Boolean),
    Column("extent", NullType),
    Column("spatial_index", Boolean),
)


t_raster_overviews = Table(
    "raster_overviews",
    metadata,
    Column("o_table_catalog", String),
    Column("o_table_schema", String),
    Column("o_table_name", String),
    Column("o_raster_column", String),
    Column("r_table_catalog", String),
    Column("r_table_schema", String),
    Column("r_table_name", String),
    Column("r_raster_column", String),
    Column("overview_factor", Integer),
)


class SpatialRefSy(Base):
    __tablename__ = "spatial_ref_sys"
    __table_args__ = (CheckConstraint("(srid > 0) AND (srid <= 998999)"),)

    srid = Column(Integer, primary_key=True)
    auth_name = Column(String(256))
    auth_srid = Column(Integer)
    srtext = Column(String(2048))
    proj4text = Column(String(2048))


# ---------------------------------------------
# Start of tables that might contain archaeological data
# ---------------------------------------------

# ---------------------------------------------
# A dictionary table with definitions of various terms used throughout the database. This schema only has a Swedish dictionary table, although it is likely that other schemata will be n-lingual
# ---------------------------------------------
class Definition(Base):
    __tablename__ = "Definition"

    # Primary key. This looks to be the full-fledged range of every MetaId in the database, meaning any MetaId in other tables should refer back to this list of definitions.
    MetaId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('definition_metaid_seq'::regclass)"),
    )
    # A reference to the dictionary version in which the term was introducted. A foreign key to the table VersionReferenceId
    VersionReferenceId = Column(ForeignKey("VersionReference.VersionReferenceId"))
    # The term that is being defined
    Name = Column(String(50), nullable=False)
    # The definition of the term. This is not always consistent; sometimes it's empty, sometimes it contains metadata. The 'help' field contains many definitions.
    Description = Column(String(100))
    # A tooltip for the software. Helps the user enter the correct data.
    ToolTip = Column(String(50))
    # A similar field to Description in that it contains definitions of terms. Not always used and seems like it needs to be merged with Description.
    Help = Column(Text)
    # Either NULL or 0; rarely used and not related to any other field.
    ExternalId = Column(Integer)
    # A boolean which is always false. Maybe there are Intrasis database instances where the 'system' adds rows here, but not in this database.
    IsSystemDefinition = Column(Boolean, nullable=False)

    VersionReference = relationship("VersionReference")
    Event = relationship("Event", secondary="DefinitionEventRel")


# ---------------------------------------------
# A table that extends the Definition table to include more information about the available attributes for objects.
# ---------------------------------------------
class AttributeDef(Definition):
    __tablename__ = "AttributeDef"

    # Relates back to the Definition table where one can find more thorough definitions of the attributes.
    MetaId = Column(ForeignKey("Definition.MetaId"), primary_key=True, index=True)
    # The definition of the attribute
    Label = Column(String(30), nullable=False)
    # Boolean to check if the user can enter multiple attriutes per object. This is a feature for the software and is only true a handful of times
    MultipleAllowed = Column(Boolean, nullable=False, server_default=text("false"))
    # Boolean to check if any value can be entered, the alternative being controlled by ranges defined in the software most likely
    FreeValueAllowed = Column(Boolean, nullable=False, server_default=text("false"))
    # Boolean to check for nullabillity
    NotNull = Column(Boolean, nullable=False, server_default=text("true"))
    # Data type for the attribute
    DataType = Column(String(35), nullable=False)
    # Unit of measurement
    Unit = Column(String(20))
    # Max length for data entry in Intrasis for a given attribute. This is not a PostgreSQL constraint though.
    Length = Column(SmallInteger)


# ---------------------------------------------
# A table listing the current running number for various number sequences, identified by a relation to the Definition table via MetaId. This is used to hold the next find number, unit number and so on. There are only 6 rows in this table, referencing Definitions 131, 152, 119, 137 and 141
# ---------------------------------------------
class Counter(AttributeDef):
    __tablename__ = "Counter"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Defines the kind of attribute that is being counted, like find number.
    MetaId = Column(ForeignKey("AttributeDef.MetaId"), primary_key=True)
    # The current number in a sequence for a given attribute, like current highest find number. Note that this is not the next free number in the sequence, but the highest currently used number.
    CurrentObjectId = Column(Integer, nullable=False, server_default=text("0"))


# ---------------------------------------------
# A table that extends the Definition table to include more information about GeoObjects.
# ---------------------------------------------
class GeoObjectDef(Definition):
    __tablename__ = "GeoObjectDef"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Denotes the definitions that are being extended, like point and polygon definitions.
    MetaId = Column(ForeignKey("Definition.MetaId"), primary_key=True)
    # This is the integer code used when recording geodata in the field for Intrasis import. Ranges from 0 to 5 in this database.
    Code = Column(String(2), nullable=False)
    # String description of the values in the Code field.
    Type = Column(String(20), nullable=False)
    # A boolean for the software. No implications for archaeological data.
    IsVisible = Column(Boolean, nullable=False, server_default=text("true"))

    ObjectDef = relationship("ObjectDef", secondary="GeoObjectRule")


# ---------------------------------------------
# A table that extends the Definition table to include more information about InfoGroups. Those look to be system variables for the software (InfoGroup System, InfoGroup Kontext, InfoGroup Observation, InfoGroup Beskrivning)
# ---------------------------------------------
class InfoGroupDef(Definition):
    __tablename__ = "InfoGroupDef"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Denotes the InfoGroup being described.
    MetaId = Column(ForeignKey("Definition.MetaId"), primary_key=True)
    # Boolean. Visibility in some parts of the software.
    IsVisible = Column(Boolean, nullable=False, server_default=text("true"))
    # Probably just a hard coded ordering for some purposes in the software.
    InfoGroupOrder = Column(SmallInteger, nullable=False)


# ---------------------------------------------
# A table that extends the Definition table to include more information about Objects. The most pertinent for archaeological data are the codes in the Code field, indicating the type of object surveyed in the field.
# This is not a static table as rows are only made for objects that call for a row in ObjectDef.
# ---------------------------------------------
class ObjectDef(Definition):
    __tablename__ = "ObjectDef"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Links to a description for the object in question.
    MetaId = Column(ForeignKey("Definition.MetaId"), primary_key=True)
    # The Code used to indicate the kind of feature surveyed (A, O, U, etc.)
    Code = Column(String(10), nullable=False)
    # The PublicId of the object.
    PublicId = Column(Integer, nullable=False)
    # Color - visualization in the software.
    Color = Column(Integer, nullable=False, server_default=text("0"))
    # Another column for the software to function.
    CanAddAttribute = Column(Boolean, nullable=False, server_default=text("false"))
    # Another column for the software to function.
    IsVisible = Column(Boolean, nullable=False, server_default=text("true"))


# ---------------------------------------------
# A table that extends the ObjectDef table to include more information about objects. As ObjectDef is an extension of Definition, this also extends those core definitions identified by MetaId.
# The information in this table seems to be mostly system info for the Intrasis software rather than archivable archaeological data.
# ---------------------------------------------
class ClassDef(ObjectDef):
    __tablename__ = "ClassDef"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Links to a description for the object in question.
    MetaId = Column(ForeignKey("ObjectDef.MetaId"), primary_key=True)
    # Another column for the software to function.
    CanAddSubclass = Column(Boolean, nullable=False, server_default=text("true"))
    # Another column for the software to function.
    SpecialRegIsActive = Column(Boolean, nullable=False, server_default=text("true"))
    # Another column for the software to function.
    InfoGroupDefId = Column(ForeignKey("InfoGroupDef.MetaId"), nullable=False)
    # Another column for the software to function.
    IsUserEditable = Column(Boolean, nullable=False, server_default=text("true"))
    # Another column for the software to function.
    GetHighId = Column(Boolean, nullable=False)
    # Another column for the software to function.
    ShowAsObjectInfo = Column(ForeignKey("AttributeDef.MetaId"))
    # Another column for the software to function.
    SubClassDesignatorId = Column(ForeignKey("Definition.MetaId"))
    # Another column for the software to function.
    ClassOrder = Column(SmallInteger, nullable=False)

    InfoGroupDef = relationship("InfoGroupDef")
    AttributeDef = relationship("AttributeDef")
    Definition = relationship("Definition")
    ObjectDef = relationship("ObjectDef", secondary="SubClassDef")


# ---------------------------------------------
# A table that extends the Definition table to include more information about relations.
# This table is probably only used in the software, as it has no additional information about the data compared to the MetaId rows in Definition.
# ---------------------------------------------
class RelationTypeDef(Definition):
    __tablename__ = "RelationTypeDef"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Links to a description for the relation in question.
    MetaId = Column(ForeignKey("Definition.MetaId"), primary_key=True)
    # Description of the relation in question. There are 3 rows in the database - Context, Space, Time.
    Label = Column(String(30), nullable=False)


# ---------------------------------------------
# A table that extends the Definition table to add some synonyms and alternative definitions for certain rows in the Definition table.
# Things like ENG-SWE translations and shorter/longer descriptions compared to the ones with matching MetaIds in the Definition table.
# ---------------------------------------------
class AlternativeDef(Definition):
    __tablename__ = "AlternativeDef"
    __table_args__ = (
        Index("AlternativeDef_IX_AlternativeDef", "MetaId", "Value", unique=True),
    )

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Links to a definition for the alternative definition in question.
    MetaId = Column(ForeignKey("Definition.MetaId"), primary_key=True)
    AttributeDefId = Column(ForeignKey("AttributeDef.MetaId"), nullable=False)
    Value = Column(String(250), nullable=False)

    AttributeDef = relationship("AttributeDef")


# ---------------------------------------------
# A table that extends the Definition table to add more granularity to the way relations are described.
# Basically splits the 'ParentRelationType / ChildRelationType' string in Definition into two substrings describing the parent and child relationship separately
# ---------------------------------------------
class RelationDef(Definition):
    __tablename__ = "RelationDef"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Links to a definition for the relation in question.
    MetaId = Column(ForeignKey("Definition.MetaId"), primary_key=True)
    # The description of the parent relationship
    ParentText = Column(String(30), nullable=False)
    # The description of the child relationship
    ChildText = Column(String(30), nullable=False)
    # A Boolean to check whether the parent and child relation is equal... I assume. Should be True for MetaId 1597 in that case, but it is not.
    Equal = Column(Boolean, nullable=False)
    # A more generalized class for relation types, listed in the RelationTypeDef table.
    RelationTypeDefId = Column(ForeignKey("RelationTypeDef.MetaId"))

    RelationTypeDef = relationship("RelationTypeDef")


# ---------------------------------------------
# The next three tables are systems tables that doesn't contain any archaeological data
# ---------------------------------------------
class AttributeMember(Base):
    __tablename__ = "AttributeMember"
    __table_args__ = (UniqueConstraint("ObjectDefId", "AttributeDefId"),)

    ObjectDefId = Column(ForeignKey("ObjectDef.MetaId"), nullable=False)
    AttributeDefId = Column(ForeignKey("AttributeDef.MetaId"), nullable=False)
    AttributeOrder = Column(SmallInteger, nullable=False)
    IsVisible = Column(Boolean, nullable=False, server_default=text("true"))
    DefaultValue = Column(String(250))
    ControlAttributeId = Column(ForeignKey("AttributeDef.MetaId"))
    AttributeMemberId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('attributememberid_seq'::regclass)"),
    )

    AttributeDef = relationship(
        "AttributeDef",
        primaryjoin="AttributeMember.AttributeDefId == AttributeDef.MetaId",
    )
    AttributeDef1 = relationship(
        "AttributeDef",
        primaryjoin="AttributeMember.ControlAttributeId == AttributeDef.MetaId",
    )
    ObjectDef = relationship("ObjectDef")


t_GeoObjectRule = Table(
    "GeoObjectRule",
    metadata,
    Column(
        "GeoObjDefId",
        ForeignKey("GeoObjectDef.MetaId"),
        primary_key=True,
        nullable=False,
    ),
    Column(
        "ObjectDefId", ForeignKey("ObjectDef.MetaId"), primary_key=True, nullable=False
    ),
)


class AlternativeMember(Base):
    __tablename__ = "AlternativeMember"

    AlternativeDefId = Column(ForeignKey("AlternativeDef.MetaId"), nullable=False)
    SelectionId = Column(ForeignKey("AlternativeDef.MetaId"))
    IsVisible = Column(Boolean, nullable=False, server_default=text("true"))
    AlternativeMemberId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('alternativememberid_seq'::regclass)"),
    )
    AttributeMemberId = Column(ForeignKey("AttributeMember.AttributeMemberId"))

    AlternativeDef = relationship(
        "AlternativeDef",
        primaryjoin="AlternativeMember.AlternativeDefId == AlternativeDef.MetaId",
    )
    AttributeMember = relationship("AttributeMember")
    AlternativeDef1 = relationship(
        "AlternativeDef",
        primaryjoin="AlternativeMember.SelectionId == AlternativeDef.MetaId",
    )


# ---------------------------------------------
# A table documenting the relations in the database.
# ---------------------------------------------
class RelationRule(Base):
    __tablename__ = "RelationRule"

    # MetaId is the primary key field for the Definition table and is used widely in the database. It is not always directly related to Definition as it is sometimes routed through tables linked to Definition, but the MetaId number should always have a corresponding row in Definition.
    # Refers to the definition of the relation in question.
    MetaId = Column(ForeignKey("RelationDef.MetaId"), primary_key=True, nullable=False)
    # The ObjectId of the parent.
    ParentId = Column(ForeignKey("ClassDef.MetaId"), primary_key=True, nullable=False)
    # The ObjectId of the child.
    ChildId = Column(ForeignKey("ClassDef.MetaId"), primary_key=True, nullable=False)
    # A field for the software.
    IsVisible = Column(Boolean, nullable=False, server_default=text("true"))
    # A Fiield for the software.
    RelCode = Column(String(10))

    ClassDef = relationship(
        "ClassDef", primaryjoin="RelationRule.ChildId == ClassDef.MetaId"
    )
    RelationDef = relationship("RelationDef")
    ClassDef1 = relationship(
        "ClassDef", primaryjoin="RelationRule.ParentId == ClassDef.MetaId"
    )


# ---------------------------------------------
# A system table for the software
# ---------------------------------------------
t_SubClassDef = Table(
    "SubClassDef",
    metadata,
    Column("MetaId", ForeignKey("ObjectDef.MetaId"), primary_key=True),
    Column("ClassId", ForeignKey("ClassDef.MetaId"), nullable=False),
)

# ---------------------------------------------
# A system table for the software
# ---------------------------------------------
class SymbolDef(Base):
    __tablename__ = "SymbolDef"

    SymbolId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('symbolid_seq'::regclass)"),
    )
    MetaId = Column(ForeignKey("Definition.MetaId"), nullable=False)
    ClassId = Column(ForeignKey("ClassDef.MetaId"), nullable=False)
    GeoDefId = Column(ForeignKey("GeoObjectDef.MetaId"), nullable=False)
    Code = Column(CHAR(1))
    Font = Column(String(150))
    IconReference = Column(String(50))
    SymbolSize = Column(Float(53), nullable=False)
    SymbolIndex = Column(SmallInteger, nullable=False)
    Color = Column(Integer, nullable=False)
    Type = Column(String(40), nullable=False)
    BorderWidth = Column(Float(53))
    BorderColor = Column(Integer)
    OffsetX = Column(Float(53), nullable=False, server_default=text("0"))
    OffsetY = Column(Float(53), nullable=False, server_default=text("0"))

    ClassDef = relationship("ClassDef")
    GeoObjectDef = relationship("GeoObjectDef")
    Definition = relationship("Definition")


# ---------------------------------------------
# A system table for the software
# ---------------------------------------------
class GraduateSymbolDef(Base):
    __tablename__ = "GraduateSymbolDef"

    Id = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('graduatesymboldef_id_seq'::regclass)"),
    )
    SymbolDefId = Column(ForeignKey("SymbolDef.SymbolId"), nullable=False)
    RangeStart = Column(Float(53), nullable=False)
    RangeEnd = Column(Float(53), nullable=False)
    Label = Column(String(255), nullable=False)
    StartValue = Column(
        Integer, nullable=False, comment="The start value as size or color."
    )
    EndValue = Column(
        Integer, nullable=False, comment="The end value as size or color."
    )

    SymbolDef = relationship("SymbolDef")


# ---------------------------------------------
# The main table for incoming data.
# ---------------------------------------------
class Object(Base):
    __tablename__ = "Object"
    __table_args__ = (
        Index("cover_object", "ObjectId", "ClassId"),
        Index("ix_objectid_object", "ObjectId", "ClassId"),
    )

    # The ID of the object.
    ObjectId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('object_objectid_seq'::regclass)"),
    )
    # The ID of the class of the object. Foreign key to ClassDef
    ClassId = Column(ForeignKey("ClassDef.MetaId"), nullable=False, index=True)
    # The ID of the class of the object. Foreign key to SubClassDef
    SubClassId = Column(ForeignKey("SubClassDef.MetaId"))
    # The PublicID of the object. Not entirely clear why this is needed alongside the ObjectId
    PublicId = Column(Integer, nullable=False, unique=True)
    # The name of the object
    Name = Column(String(50))
    # The site ID. This is a recursive link back to the Object table meaning that the Site can be queried by the clause WHERE ObjectId = SiteId
    SiteId = Column(ForeignKey("Object.ObjectId"))
    # This is a field for objects that originate from imported files, like total station survey files.
    Origin = Column(Integer)

    ClassDef = relationship("ClassDef")
    parent = relationship("Object", remote_side=[ObjectId])
    SubClassDef = relationship("SubClassDef")


# ---------------------------------------------
# An extension of the Object table for GeoObject. Replicates GeoObjects from Objects and records further information like spatial data type and a WKB definition of the geometry.
# ---------------------------------------------
class GeoObject(Object):
    __tablename__ = "GeoObject"

    # The ID of the Object
    ObjectId = Column(ForeignKey("Object.ObjectId"), primary_key=True)
    # The ID of the definition of the object
    MetaId = Column(ForeignKey("GeoObjectDef.MetaId"), nullable=False)
    # System field for the software
    SymbolId = Column(Integer)
    # The spatial data definition in WKB format. Mutually exclusive with the 'the_raster' field.
    the_geom = Column(NullType)
    # The data definition of the GeoObject if it is a raster. Mutually exclusive with the 'the_geom' field.
    the_raster = Column(NullType)

    GeoObjectDef = relationship("GeoObjectDef")
    Object = relationship("Object", secondary="GeoRel")


# ---------------------------------------------
# A table listing every attribute for objects in the database.
# ---------------------------------------------
class Attribute(Base):
    __tablename__ = "Attribute"
    __table_args__ = (Index("cover_attribute", "ObjectId", "MetaId"),)

    # The ID of the attribute
    AttributeId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('attribute_attributeid_seq'::regclass)"),
    )
    # The relation back to the Definition Table via AttributeDef.
    MetaId = Column(ForeignKey("AttributeDef.MetaId"), index=True)
    # The ID of the object.
    ObjectId = Column(ForeignKey("Object.ObjectId"), nullable=False)
    # The 'label', or name of the attribute.
    Label = Column(String(50), nullable=False)

    AttributeDef = relationship("AttributeDef")
    Object = relationship("Object")


# ---------------------------------------------
# This is a metadata table for any binary object imported into the database. In most cases these are photos and other images.
# ---------------------------------------------
class BinaryAttributeValue(Attribute):
    __tablename__ = "BinaryAttributeValue"

    # Link to the attribute table, which is then linked to the Object table. This link indicates which object a row in this table is linked to.
    AttributeId = Column(ForeignKey("Attribute.AttributeId"), primary_key=True)
    # Indicates the size and type of binary object/BLOB
    Value = Column(LargeBinary)
    # The name of the imported file, e.g. Ritn 33.jpg
    Reference = Column(String(250))
    # Type of file, e.g. image/jpeg
    MimeType = Column(String(100))


# ---------------------------------------------
# Free text attributes. Includes encoding, so the contents have to be converted to be easily readable
# ---------------------------------------------
class FreeText(Attribute):
    __tablename__ = "FreeText"

    # Link to the attribute table, which is then linked to the Object table. This link indicates which object a row in this table is linked to.
    AttributeId = Column(ForeignKey("Attribute.AttributeId"), primary_key=True)
    # The free text field. Encoding is probably converted using the currently unavailable RTF function. Gisli wrote a function to convert this field into a legible one.
    Text = Column(Text, nullable=False)


# ---------------------------------------------
# A table listing all database events relating to objects in the Object table. Appears to be extremely thorough, listing not just edits and
# creations but also any act of viewing the table.
# ---------------------------------------------
class Event(Base):
    __tablename__ = "Event"

    # The ID of the event.
    EventId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('event_eventid_seq'::regclass)"),
    )
    # The ID of the object that the event is pertaining to.
    ObjectId = Column(ForeignKey("Object.ObjectId"), nullable=False)
    # The time the event took place.
    Time = Column(DateTime, nullable=False)
    # A description of the event that took place.
    Description = Column(String(250))

    Object = relationship("Object")
    Object1 = relationship("Object", secondary="ObjectEventRel")


# ---------------------------------------------
# A list of deleted objects with a MetaID (ie definition objects)... I think. This table is a subclass of the Event table, but has a MetaID
# field which the Events table does not. DeletedObjects is where deleted objects go obviously.
# ---------------------------------------------
class DeletedMetaObject(Event):
    __tablename__ = "DeletedMetaObject"

    # Event ID. Relates back to the parent table Events.
    EventId = Column(ForeignKey("Event.EventId"), primary_key=True)
    # MetaID, linking back to Definition. Interestingly,
    MetaId = Column(Integer, nullable=False)
    # Name of the deleted object, probably
    Name = Column(String(50), nullable=False)
    # Type of the deleted object, probably
    MetaObjectType = Column(String(50), nullable=False)


# ---------------------------------------------
# A table for objects that were deleted from the Object table. Subclass of event, as deletions are logged there.
# ---------------------------------------------
class DeletedObject(Event):
    __tablename__ = "DeletedObject"

    # The ID of the deletion event.
    EventId = Column(ForeignKey("Event.EventId"), primary_key=True)
    # The PublidID of the object.
    PublicId = Column(Integer, nullable=False)
    # The ObjectID of the object.
    ObjectId = Column(Integer, nullable=False)
    # The class of the object.
    Class = Column(String(50), nullable=False)
    # The subclass of the object.
    SubClass = Column(String(50))


# ---------------------------------------------
# ObjectRel is a linking table between related objects in the Object table based on a parent-child ontology.
# ---------------------------------------------
class ObjectRel(Base):
    __tablename__ = "ObjectRel"

    # The ID of the 'parent' entity, generally speaking the unit a sample is taken from, or some other obvious parent-child relationship
    ParentId = Column(ForeignKey("Object.ObjectId"), primary_key=True, nullable=False)
    # The ID of the 'child' entity, such as a sample taken from a parent unit.
    ChildId = Column(ForeignKey("Object.ObjectId"), primary_key=True, nullable=False)
    # The type of relationship
    MetaId = Column(ForeignKey("RelationDef.MetaId"), primary_key=True, nullable=False)
    # Relationships to other tables
    Object = relationship("Object", primaryjoin="ObjectRel.ChildId == Object.ObjectId")
    RelationDef = relationship("RelationDef")
    Object1 = relationship(
        "Object", primaryjoin="ObjectRel.ParentId == Object.ObjectId"
    )


# ---------------------------------------------
# Provides values for attributes associated with an object in the Attributes table. The way this works is that the Attribute table has the name of the variable name,
# like 'site', whereas AttributeValue has the actual data, like 'Stonehenge'.
# ---------------------------------------------
class AttributeValue(Base):
    __tablename__ = "AttributeValue"

    # The primary key for the table
    AttributeValueId = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('attributevalue_attributevalueid_seq'::regclass)"),
    )
    # Foreign key to the Attribute table. This is the 'lookup' to the variable name
    AttributeId = Column(
        ForeignKey("Attribute.AttributeId"), nullable=False, index=True
    )
    # This is the variable value
    Value = Column(String(260))
    # Unit of measurement, where appropriate
    Unit = Column(String(30))

    Attribute = relationship("Attribute")


# ---------------------------------------------
# A linking table between the Event and Definition tables
# ---------------------------------------------
t_DefinitionEventRel = Table(
    "DefinitionEventRel",
    metadata,
    # Foreign key to the Event table
    Column("EventId", ForeignKey("Event.EventId"), primary_key=True, nullable=False),
    # Foreign key to the Definition table
    Column(
        "DefinitionId",
        ForeignKey("Definition.MetaId"),
        primary_key=True,
        nullable=False,
    ),
)


# ---------------------------------------------
# A linking table between objects in the Object table and their corresponding spatial objects in the GeoObjects table
# ---------------------------------------------
t_GeoRel = Table(
    "GeoRel",
    metadata,
    # Foreign key to the Object table
    Column(
        "ObjectId",
        ForeignKey("Object.ObjectId"),
        primary_key=True,
        nullable=False,
        index=True,
    ),
    # Foreign key to the GeoObject table
    Column(
        "GeoObjectId",
        ForeignKey("GeoObject.ObjectId"),
        primary_key=True,
        nullable=False,
    ),
)


# ---------------------------------------------
# This table holds GeoObjects that were deleted from the GeoObject table.
# The structure is identical to the GeoObject table, except for an additional
# Event field holding a foreign key to the Event table. This logs the deletion
# event.
# ---------------------------------------------
class HistoricGeoObject(Base):
    __tablename__ = "HistoricGeoObject"

    # Refer to GeoObject for field definitions
    Id = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('historicgeoobject_id_seq'::regclass)"),
    )
    ObjectId = Column(Integer, nullable=False)
    EventId = Column(ForeignKey("Event.EventId"), nullable=False)
    MetaId = Column(Integer, nullable=False)
    SymbolId = Column(Integer)
    the_geom = Column(NullType)
    the_raster = Column(NullType)

    Event = relationship("Event")


# ---------------------------------------------
# A linking table between objects and events. Contains information about the
# events that resulted in created objects in the Object table
# ---------------------------------------------
t_ObjectEventRel = Table(
    "ObjectEventRel",
    metadata,
    # Foreign key to the Event table
    Column("EventId", ForeignKey("Event.EventId"), primary_key=True, nullable=False),
    # Foreign key to the Object table
    Column("ObjectId", ForeignKey("Object.ObjectId"), primary_key=True, nullable=False),
)
