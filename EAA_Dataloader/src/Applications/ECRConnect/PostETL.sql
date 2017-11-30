/* Updates countries codes in the ECR Risks from ISO 2 standard to Full Names. 
 * Assigns the risks classification. 
 * Calculates the risks average value by country and risk classification.
 * Fixes the risk names for readability. */

/* Fix the Conuntry Names */
update {schemaName}.{tableName}
	set  country =  iso.countryname
from {schemaName}.{tableName} risks
	inner join common.tbliso3166 iso on iso.iso3166 = risks.country;

update {schemaName}.{tableName}_history
	set  country =  iso.countryname
from {schemaName}.{tableName}_history risks
	inner join common.tbliso3166 iso on iso.iso3166 = risks.country;

/* Update the risks clasification */
update {schemaName}.{tableName}
  set risk_class =  xcls.class_name
from {schemaName}.{tableName} risks
  inner join {schemaName}.{tableName}_xref_class xcls on xcls.risk_name = risks.risk_name;

/* Calculate the risks clasification average*/
update {schemaName}.{tableName}
  set risk_class_avg =  avgs.class_avg
from {schemaName}.{tableName} risks
  inner join (select country, risk_name, risk_class, avg(risk_value) over(partition by country, risk_class) as class_avg from {schemaName}.{tableName} risks) avgs 
    on avgs.country = risks.country
        and avgs.risk_name = risks.risk_name
        and avgs.risk_class = risks.risk_class;

/* Fix the risks names */
update {schemaName}.{tableName}
  set risk_name =  xcls.risk_desc
from {schemaName}.{tableName} risks
  inner join {schemaName}.{tableName}_xref_class xcls on xcls.risk_name = risks.risk_name;

update {schemaName}.{tableName}_history
  set risk_name =  xcls.risk_desc
from {schemaName}.{tableName}_history risks
  inner join {schemaName}.{tableName}_xref_class xcls on xcls.risk_name = risks.risk_name;


/* Drop the cross reference */
DROP TABLE IF EXISTS {schemaName}.{tableName}_xref_class CASCADE;