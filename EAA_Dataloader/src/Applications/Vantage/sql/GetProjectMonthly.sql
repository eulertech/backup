/*
	Unconventional Monthly Data (Project Level)
*/
select	da.AssetName,
		dp.ProjectName,
		dd.DateDate,
		dps.PriceScenarioDescription,
		dc.CompanyName,
		fppm.MonthlyRigs,
		fppm.MonthlyWellsDrilled
from AC.FactProjectPeriodicMonthly fppm
	inner join AC.DimDate dd on dd.DateID = fppm.DateID
	inner join AC.DimProject dp on dp.ProjectId = fppm.ProjectID
	inner join AC.DimAsset da on da.AssetId = fppm.FieldID
	inner join AC.DimPriceScenario dps on dps.PriceScenarioID = fppm.PriceScenarioID
	inner join AC.DimCompany dc on dc.CompanyID = fppm.CompanyID
where fppm.SnapshotID = 1