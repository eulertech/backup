-- Table: eaa_dev.Phoenix_Documents

-- DROP TABLE eaa_dev.Phoenix_Documents;

CREATE TABLE eaa_dev.Phoenix_Documents
(
  id serial NOT NULL,
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

