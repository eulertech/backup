-- @WbResult variablelist_51
SELECT count(distinct(varid))
FROM spectre.variablelist_51 
 ;
 

select top 1000 RIGHT(variableid,LEN(variableid)-1) as variableid from eaa_analysis.feature_selection_matrix_csv;


select * from spectre.VariableListIds
where listguid = 'timeseries_viz';

SELECT * FROM  spectre.variablelistheader WHERE ListId = 51;

select max(len(variableid)) from eaa_analysis.feature_selection_matrix_csv;

delete spectre.variablelistids where listid in (51,52);
select top 50 * from STL_LOAD_ERRORS order by starttime desc ;


select * from hindsight_prod.series_attributes where series_id = '124224421';
