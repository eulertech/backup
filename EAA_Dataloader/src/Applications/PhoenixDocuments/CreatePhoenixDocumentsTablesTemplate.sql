--Script to create Phoenix_Documents Tables
DROP TABLE IF EXISTS  {schemaName}.{tableName};

CREATE TABLE {schemaName}.{tableName}
(
  attr_id integer,
  TitleHtml character varying(4000),
  BodyHtml text,
  PublishDate character varying(10),
  TaxonomyName  character varying(128),
  TaxonomyValueId integer
)
WITH (
  OIDS=FALSE
);

