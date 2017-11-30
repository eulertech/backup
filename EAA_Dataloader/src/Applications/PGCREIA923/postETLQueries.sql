-- Script to create the consolidated Form-923 tables

--Page 6 Data Consolidation
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p6plantframe;
select * into {schemaName}.{tableName}L2p6plantframe from
(
select year,'Yearly' as Frequency,NULL as month, eiaplantid as plantid, plantstate, sector as sectornumber, sectorname, naicscode, plantname, combinedheatandpowerstatus,reportingfrequency,nameplatecapacitymw from {schemaName}.{tableName}p6plantframe2011
union
select year,'Yearly' as Frequency,NULL as month, plantid, plantstate, sectornumber,NULL as sectorname, naicscode, plantname, combinedheatandpowerstatus, reportingfrequency, NULL as nameplatecapacitymw from {schemaName}.{tableName}p6plantframe2012
union
select year,'Yearly' as Frequency,NULL as month, eiaplantid as plantid,plantstate,sector as sectornumber, sectorname, naicscode, plantname, combinedheatandpowerstatus, reportingfrequency, NULL as nameplatecapacitymw from {schemaName}.{tableName}p6plantframe2013
union
select year,'Yearly' as Frequency,NULL as month, plantid, plantstate, sectornumber,NULL as sectorname, naicscode, plantname, combinedheatandpowerstatus, reportingfrequency, NULL as nameplatecapacitymw from {schemaName}.{tableName}p6plantframe2014to15
union
select year,'Monthly' as Frequency, month, plantid, plantstate, sectornumber,NULL as sectorname, naicscode, plantname, combinedheatandpowerstatus, reportingfrequency, NULL as nameplatecapacitymw from {schemaName}.{tableName}p6plantframe2016onwards
);

--Page 5 Data Consolidation
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p5fuelreceiptsandcosts;
select * into {schemaName}.{tableName}L2p5fuelreceiptsandcosts from
(
select year,month,plantid,plantname,plantstate,purchasetype,contractexpirationdate,energy_source,fuel_group,coalminetype,
coalminestate,coalminecounty,coalminemshaid,coalminename,supplier,quantity,averageheatcontent,averagesulfurcontent,averageashcontent,NULL as averagemercurycontent,fuel_cost,
regulated,operatorname,operatorid,reportingfrequency,primarytransportationmode,secondarytransportationmode,naturalgastransportationservice from {schemaName}.{tableName}p5fuelreceiptsandcostsnomercury
union
select year,month,plantid,plantname,plantstate,purchasetype,contractexpirationdate,energy_source,fuel_group,coalminetype,
coalminestate,coalminecounty,coalminemshaid,coalminename,supplier,quantity,averageheatcontent,averagesulfurcontent,averageashcontent,averagemercurycontent,fuel_cost,
regulated,operatorname,operatorid,reportingfrequency,primarytransportationmode,secondarytransportationmode,naturalgastransportationservice from {schemaName}.{tableName}p5fuelreceiptsandcostswithmercury
);

--Source Disposition Data Consolidation
DROP TABLE IF EXISTS {schemaName}.{tableName}L2sourcedisposition;
select * into {schemaName}.{tableName}L2sourcedisposition from
(
select year,utilityid,utilityname,sectorcode,plantcode,chpplant,plantname,plantstate,grossgeneration,incomingelectricity,totalsource
,stationuse,directuse,totalfacilityuse,retailsales,salesforresale,outgoingelectricity,total_disposition,revenuefromresale
,NULL as typesofincomingelectricity,NULL as typesofoutgoingelectricity,NULL as tollingagreements from {schemaName}.{tableName}sourcedispositiontill2011
union
select year,utilityid,utilityname,sectorcode,plantcode,chpplant,plantname,plantstate,grossgeneration,incomingelectricity,totalsource
,stationuse,directuse,totalfacilityuse,retailsales,salesforresale,outgoingelectricity,total_disposition,revenuefromresale
,typesofincomingelectricity,typesofoutgoingelectricity, tollingagreements from {schemaName}.{tableName}sourcedisposition2012onwards
);

--Page 1 Month transposition
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p1genfuel;
select * into {schemaName}.{tableName}L2p1genfuel from
(
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 1 as month
,quantityjanuary as quantity
,elec_quantityjanuary as elec_quantity
,mmbtuper_unitjanuary as mmbtuper_unit
,tot_mmbtujanuary as tot_mmbtu
,elec_mmbtujanuary as elec_mmbtu
,netgenjanuary as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 2 as month
,quantityfebruary as quantity
,elec_quantityfebruary as elec_quantity
,mmbtuper_unitfebruary as mmbtuper_unit
,tot_mmbtufebruary as tot_mmbtu
,elec_mmbtufebruary as elec_mmbtu
,netgenfebruary as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 3 as month
,quantitymarch as quantity
,elec_quantitymarch as elec_quantity
,mmbtuper_unitmarch as mmbtuper_unit
,tot_mmbtumarch as tot_mmbtu
,elec_mmbtumarch as elec_mmbtu
,netgenmarch as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 4 as month
,quantityapril as quantity
,elec_quantityapril as elec_quantity
,mmbtuper_unitapril as mmbtuper_unit
,tot_mmbtuapril as tot_mmbtu
,elec_mmbtuapril as elec_mmbtu
,netgenapril as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 5 as month
,quantitymay as quantity
,elec_quantitymay as elec_quantity
,mmbtuper_unitmay as mmbtuper_unit
,tot_mmbtumay as tot_mmbtu
,elec_mmbtumay as elec_mmbtu
,netgenmay as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 6 as month
,quantityjune as quantity
,elec_quantityjune as elec_quantity
,mmbtuper_unitjune as mmbtuper_unit
,tot_mmbtujune as tot_mmbtu
,elec_mmbtujune as elec_mmbtu
,netgenjune as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 7 as month
,quantityjuly as quantity
,elec_quantityjuly as elec_quantity
,mmbtuper_unitjuly as mmbtuper_unit
,tot_mmbtujuly as tot_mmbtu
,elec_mmbtujuly as elec_mmbtu
,netgenjuly as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 8 as month
,quantityaugust as quantity
,elec_quantityaugust as elec_quantity
,mmbtuper_unitaugust as mmbtuper_unit
,tot_mmbtuaugust as tot_mmbtu
,elec_mmbtuaugust as elec_mmbtu
,netgenaugust as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 9 as month
,quantityseptember as quantity
,elec_quantityseptember as elec_quantity
,mmbtuper_unitseptember as mmbtuper_unit
,tot_mmbtuseptember as tot_mmbtu
,elec_mmbtuseptember as elec_mmbtu
,netgenseptember as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 10 as month
,quantityoctober as quantity
,elec_quantityoctober as elec_quantity
,mmbtuper_unitoctober as mmbtuper_unit
,tot_mmbtuoctober as tot_mmbtu
,elec_mmbtuoctober as elec_mmbtu
,netgenoctober as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 11 as month
,quantitynovember as quantity
,elec_quantitynovember as elec_quantity
,mmbtuper_unitnovember as mmbtuper_unit
,tot_mmbtunovember as tot_mmbtu
,elec_mmbtunovember as elec_mmbtu
,netgennovember as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
union
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1,naicscode,eiasectornumber,sectorname,
reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel, 12 as month
,quantitydecember as quantity
,elec_quantitydecember as elec_quantity
,mmbtuper_unitdecember as mmbtuper_unit
,tot_mmbtudecember as tot_mmbtu
,elec_mmbtudecember as elec_mmbtu
,netgendecember as netgen
,totalfuelconsumptionquantity,electricfuelconsumptionquantity,totalfuelconsumptionmmbtu,elecfuelconsumptionmmbtu,netgeneration_megawatthours,year
from {schemaName}.{tableName}p1genfuel
);

--Page 2 Month transposition
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p2stocks;
select * into {schemaName}.{tableName}L2p2stocks from
(
select region_name, year, 1 as month
,coal_jan as coal
,oil_jan as oil
,petcoke_jan as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 2 as month
,coal_feb as coal
,oil_feb as oil
,petcoke_feb as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 3 as month
,coal_mar as coal
,oil_mar as oil
,petcoke_mar as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 4 as month
,coal_apr as coal
,oil_apr as oil
,petcoke_apr as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 5 as month
,coal_may as coal
,oil_may as oil
,petcoke_may as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 6 as month
,coal_jun as coal
,oil_jun as oil
,petcoke_jun as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 7 as month
,coal_jul as coal
,oil_jul as oil
,petcoke_jul as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 8 as month
,coal_aug as coal
,oil_aug as oil
,petcoke_aug as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 9 as month
,coal_sep as coal
,oil_sep as oil
,petcoke_sep as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 10 as month
,coal_oct as coal
,oil_oct as oil
,petcoke_oct as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 11 as month
,coal_nov as coal
,oil_nov as oil
,petcoke_nov as petcoke
from {schemaName}.{tableName}p2stocks
union
select region_name, year, 12 as month
,coal_dec as coal
,oil_dec as oil
,petcoke_dec as petcoke
from {schemaName}.{tableName}p2stocks
);

--Page 3 Month Transposition
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p3boilerfuel;
select * into {schemaName}.{tableName}L2p3boilerfuel from
(
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 1 as month
,quantityoffuelconsumedjanuary as quantityoffuelconsumed
,mmbtuperunitjanuary as mmbtuperunit
,sulfurcontentjanuary as sulfurcontent
,ashcontentjanuary as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 2 as month
,quantityoffuelconsumedfebruary as quantityoffuelconsumed
,mmbtuperunitfebruary as mmbtuperunit
,sulfurcontentfebruary as sulfurcontent
,ashcontentfebruary as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 3 as month
,quantityoffuelconsumedmarch as quantityoffuelconsumed
,mmbtuperunitmarch as mmbtuperunit
,sulfurcontentmarch as sulfurcontent
,ashcontentmarch as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 4 as month
,quantityoffuelconsumedapril as quantityoffuelconsumed
,mmbtuperunitapril as mmbtuperunit
,sulfurcontentapril as sulfurcontent
,ashcontentapril as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 5 as month
,quantityoffuelconsumedmay as quantityoffuelconsumed
,mmbtuperunitmay as mmbtuperunit
,sulfurcontentmay as sulfurcontent
,ashcontentmay as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 6 as month
,quantityoffuelconsumedjune as quantityoffuelconsumed
,mmbtuperunitjune as mmbtuperunit
,sulfurcontentjune as sulfurcontent
,ashcontentjune as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 7 as month
,quantityoffuelconsumedjuly as quantityoffuelconsumed
,mmbtuperunitjuly as mmbtuperunit
,sulfurcontentjuly as sulfurcontent
,ashcontentjuly as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 8 as month
,quantityoffuelconsumedaugust as quantityoffuelconsumed
,mmbtuperunitaugust as mmbtuperunit
,sulfurcontentaugust as sulfurcontent
,ashcontentaugust as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 9 as month
,quantityoffuelconsumedseptember as quantityoffuelconsumed
,mmbtuperunitseptember as mmbtuperunit
,sulfurcontentseptember as sulfurcontent
,ashcontentseptember as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 10 as month
,quantityoffuelconsumedoctober as quantityoffuelconsumed
,mmbtuperunitoctober as mmbtuperunit
,sulfurcontentoctober as sulfurcontent
,ashcontentoctober as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 11 as month
,quantityoffuelconsumednovember as quantityoffuelconsumed
,mmbtuperunitnovember as mmbtuperunit
,sulfurcontentnovember as sulfurcontent
,ashcontentnovember as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel, 12 as month
,quantityoffuelconsumeddecember as quantityoffuelconsumed
,mmbtuperunitdecember as mmbtuperunit
,sulfurcontentdecember as sulfurcontent
,ashcontentdecember as ashcontent
,totalfuelconsumptionquantity,year
from {schemaName}.{tableName}p3boilerfuel
);

--Page 4 Month Transposition
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p4generator;
select * into {schemaName}.{tableName}L2p4generator from
(
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 1 as month
,netgenerationjanuary as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 2 as month
,netgenerationfebruary as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 3 as month
,netgenerationmarch as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 4 as month
,netgenerationapril as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 5 as month
,netgenerationmay as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 6 as month
,netgenerationjune as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 7 as month
,netgenerationjuly as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 8 as month
,netgenerationaugust as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 9 as month
,netgenerationseptember as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 10 as month
,netgenerationoctober as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 11 as month
,netgenerationnovember as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
union
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,
naicscode,sectornumber,sectorname,generatorid,reportedprimemover, 12 as month
,netgenerationdecember as netgeneration
,netgenerationyeartodate,year
from {schemaName}.{tableName}p4generator
);

--Level 3 Queries
--Page 1
DROP TABLE IF EXISTS {schemaName}.{tableName}l3P1GenFuel;
select plantid,combinedheatandpowerplant,nuclearunitid,plantname,operatorname,operatorid,plantstate,censusregion,nercregion,reserved1
,naicscode,eiasectornumber,sectorname,reportedprimemover,reportedfueltypecode,aerfueltypecode,reserved2,reserved3,physicalunitlabel
, cast(case when quantity = '.' then NULL
                 else quantity
            end as numeric) as quantity
, cast(case when elec_quantity = '.' then NULL
                 else elec_quantity
            end as numeric) as elec_quantity
, cast(case when mmbtuper_unit = '.' then NULL
                 else mmbtuper_unit
            end as float) as mmbtuper_unit
, cast(case when tot_mmbtu = '.' then NULL
                 else tot_mmbtu
            end as numeric) as tot_mmbtu
, cast(case when elec_mmbtu = '.' then NULL
                 else elec_mmbtu
            end as numeric) as elec_mmbtu
, cast(case when netgen = '.' then NULL
                 else netgen
            end as float) as netgen
, year, month, cast(cast(month as varchar(2)) || '-15-' || cast(year as varchar(4)) as date) as date
into {schemaName}.{tableName}l3P1GenFuel
from {schemaName}.{tableName}l2p1genfuel;

--Page 2
DROP TABLE IF EXISTS {schemaName}.{tableName}l3p2stocks;
select region_name, year, month, cast(cast(month as varchar(2)) || '-15-' || cast(year as varchar(4)) as date) as date
, cast(case when coal = '.' then NULL
            when lower(coal) = 'w' then NULL
                 else coal
            end as float) as coal
, cast(case when oil = '.' then NULL
            when lower(oil) = 'w' then NULL
                 else oil
            end as float) as oil
, cast(case when petcoke = '.' then NULL
            when lower(petcoke) = 'w' then NULL
                 else petcoke
            end as float) as petcoke
into {schemaName}.{tableName}l3p2stocks
from {schemaName}.{tableName}l2p2stocks;

--Page 3
DROP TABLE IF EXISTS {schemaName}.{tableName}l3p3boilerfuel;
select cast(plantid as numeric),combinedheatandpowerplant,plantname,operatorname,cast(operatorid as numeric),plantstate,censusregion
,nercregion,cast(naicscode as numeric),cast(sectornumber as numeric),sectorname,boilerid,reportedprimemover,reportedfueltypecode,physicalunitlabel
, cast(case when quantityoffuelconsumed = '.' then NULL
                 else quantityoffuelconsumed
            end as numeric) as quantityoffuelconsumed
, cast(case when mmbtuperunit = '.' then NULL
                 else mmbtuperunit
            end as numeric) as mmbtuperunit
, cast(case when sulfurcontent = '.' then NULL
                 else sulfurcontent
            end as numeric) as sulfurcontent
, cast(case when ashcontent = '.' then NULL
                 else ashcontent
            end as numeric) as ashcontent
, cast(year as numeric), month, cast(cast(month as varchar(2)) || '-15-' || cast(year as varchar(4)) as date) as date
 into {schemaName}.{tableName}l3p3boilerfuel
 from {schemaName}.{tableName}l2p3boilerfuel;


--Page 4
DROP TABLE IF EXISTS {schemaName}.{tableName}l3p4generator;
select plantid,combinedheatandpowerplant,plantname,operatorname,operatorid,plantstate,censusregion
,nercregion,naicscode,sectornumber,sectorname,generatorid,reportedprimemover
, cast(case when netgeneration = '.' then NULL
                 else netgeneration
            end as numeric) as netgeneration
, year, month, cast(cast(month as varchar(2)) || '-15-' || cast(year as varchar(4)) as date) as date
 into {schemaName}.{tableName}l3p4generator
 from {schemaName}.{tableName}l2p4generator;

--Page 5
DROP TABLE IF EXISTS {schemaName}.{tableName}l3p5fuelreceiptsandcosts;
select month, year, cast(cast(month as varchar(2)) || '-15-' || cast(year as varchar(4)) as date) as date
,plantid, plantname, plantstate, purchasetype
, cast(case when contractexpirationdate = '.' then NULL
                 else contractexpirationdate
            end as numeric) as contractexpirationdate
,energy_source, fuel_group,coalminetype,coalminestate, coalminecounty
, cast(case when coalminemshaid = '.' then NULL
            when coalminemshaid = 'NA' then NULL
                 else coalminemshaid
            end as numeric) as coalminemshaid
,coalminename,supplier,quantity,averageheatcontent,averagesulfurcontent,averageashcontent,averagemercurycontent
, cast(case when fuel_cost = '.' then NULL
                 else fuel_cost
            end as float) as fuel_cost
,regulated,operatorname
, cast(case when operatorid = '.' then NULL
                 else operatorid
            end as numeric) as operatorid
,reportingfrequency,primarytransportationmode,secondarytransportationmode,naturalgastransportationservice
into {schemaName}.{tableName}l3p5fuelreceiptsandcosts
from {schemaName}.{tableName}l2p5fuelreceiptsandcosts;

--Page 6
DROP TABLE IF EXISTS {schemaName}.{tableName}l3p6plantframe;
select year,frequency,month
, cast(case when plantid = '.' then NULL
                 else plantid
            end as numeric) as plantid
,plantstate,sectornumber,sectorname
, cast(case when naicscode = '.' then NULL
                 else naicscode
            end as numeric) as naicscode
,plantname,combinedheatandpowerstatus,reportingfrequency
, cast(case when nameplatecapacitymw = '.' then NULL
                 else nameplatecapacitymw
            end as float) as nameplatecapacitymw
into {schemaName}.{tableName}l3p6plantframe
from {schemaName}.{tableName}l2p6plantframe;

--Page Sourcedisposition
DROP TABLE IF EXISTS {schemaName}.{tableName}l3sourcedisposition;
select year
, cast(case when utilityid = '.' then NULL
                 else utilityid
            end as numeric) as utilityid
,utilityname, plantcode, chpplant, plantname, plantstate
, cast(case when grossgeneration = '.' then NULL
                 else grossgeneration
            end as numeric) as grossgeneration
, cast(case when incomingelectricity = '.' then NULL
                 else incomingelectricity
            end as numeric) as incomingelectricity
, cast(case when totalsource = '.' then NULL
                 else totalsource
            end as numeric) as totalsource
, cast(case when stationuse = '.' then NULL
                 else stationuse
            end as numeric) as stationuse
, cast(case when directuse = '.' then NULL
                 else directuse
            end as numeric) as directuse
, cast(case when totalfacilityuse = '.' then NULL
                 else totalfacilityuse
            end as numeric) as totalfacilityuse
, cast(case when retailsales = '.' then NULL
                 else retailsales
            end as numeric) as retailsales
, cast(case when salesforresale = '.' then NULL
                 else salesforresale
            end as numeric) as salesforresale
, cast(case when outgoingelectricity = '.' then NULL
                 else outgoingelectricity
            end as numeric) as outgoingelectricity
, cast(case when total_disposition = '.' then NULL
                 else total_disposition
            end as numeric) as total_disposition
, cast(case when revenuefromresale = '.' then NULL
                 else revenuefromresale
            end as numeric) as revenuefromresale
, typesofincomingelectricity, typesofoutgoingelectricity
, cast(case when tollingagreements = '.' then NULL
                 else tollingagreements
            end as numeric) as tollingagreements
into {schemaName}.{tableName}l3sourcedisposition
from {schemaName}.{tableName}l2sourcedisposition;

--Drop l2 tables
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p1genfuel;
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p2stocks;
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p3boilerfuel;
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p4generator;
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p5fuelreceiptsandcosts;
DROP TABLE IF EXISTS {schemaName}.{tableName}L2p6plantframe;
DROP TABLE IF EXISTS {schemaName}.{tableName}L2sourcedisposition;


