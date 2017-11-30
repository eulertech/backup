-- This script is used to create the JODI Data View.

CREATE VIEW {schemaName}.vw_jodi 
AS
  SELECT country, jprod.full_name AS product, jflow.full_name AS flow, junit.description + '(' + uom + ')' AS unit,
    jp.date, jp.quantity, jqua.descriptiON AS qualifier, 'primary' AS file_source
  FROM {schemaName}.jodi_primary jp
    LEFT JOIN {schemaName}.jodi_product jprod ON jprod.name = jp.product AND jprod.file_table = 'primary'
    LEFT JOIN {schemaName}.jodi_flow jflow ON jflow.name = jp.flow AND jprod.file_table = 'primary'
    LEFT JOIN {schemaName}.jodi_units junit ON junit.name = jp.unit
    LEFT JOIN {schemaName}.jodi_qualifier jqua ON jqua.code = jp.code

  UNION ALL

  select country, jprod.full_name AS product, jflow.full_name AS flow, junit.description + '(' + uom + ')' AS unit,
    jp.date, jp.quantity, jqua.descriptiON AS qualifier, 'secondary' AS file_source
  from {schemaName}.jodi_secondary jp
    LEFT JOIN {schemaName}.jodi_product jprod ON jprod.name = jp.product AND jprod.file_table = 'secondary'
    LEFT JOIN {schemaName}.jodi_flow jflow ON jflow.name = jp.flow AND jprod.file_table = 'secondary'
    LEFT JOIN {schemaName}.jodi_units junit ON junit.name = jp.unit
    LEFT JOIN {schemaName}.jodi_qualifier jqua ON jqua.code = jp.code
 ;