/*
	Conventional and Unconventional Summary Data (Project Level)
*/
select	da.AssetName, 
		db.BasinName, 
		dp.ProjectName,
		dproj.ProjectIsSanctioned,
		dps.PriceScenarioDescription, 
		di.InflationValue, 
		dc.CountryName, 
		da.AssetIsUnconventional,
		fps.CapitalAbandonment,
		fps.CapitalDevelopmentTotal,
		fps.CapitalTotalExcludingAbandonment,
		fps.GrossTotalRevenue,
		fps.IncomeTax,
		fps.ProductionCO2Emissions,
		fps.TotalCost,
		fpr.ProjectCO2Content,
		fps.OilRecoverableReserves,
		fps.GasPointForwardBreakEvenPrice,
		fps.InternalRateOfReturn,
		fps.TotalAbandonmentCapitalPerGasEquivalent,
		fps.TotalAbandonmentCapitalPerOilEquivalent,
		fps.TotalCostPerGasEquivalent,
		fps.TotalCostPerOilEquivalent,
		fps.TotalDrillingDevelopmentCapitalPerOilEquivalent,
		fps.TotalDrillingDevelopmentCapitalPerGasEquivalent,
		fps.TotalExplorationCapitalPerOilEquivalent,
		fps.TotalExplorationCapitalPerGasEquivalent,
		fps.TotalOpexPerGasEquivalent,
		fps.TotalOpexPerOilEquivalent,
		fpr.CapitalCostPerWellCompletion,
		fpr.CapitalCostPerWellDrilling,
		fpr.CapitalCostPerWellFacilities,
		da.AssetNumberOfProjects,
		fps.DevelopmentCostPerGasEquivalent,
		fps.DevelopmentCostPerOilEquivalent
from AC.V_FactProjectSummary fps
	inner join AC.DimAsset da on da.AssetId = fps.AssetID
	inner join AC.DimPriceScenario dps on dps.PriceScenarioID = fps.PriceScenarioID
	inner join AC.DimInflation di on di.InflationId = fps.InflationId
	inner join AC.DimCountry dc on dc.CountryId = fps.CountryID
	inner join AC.DimBasin db on db.BasinId = fps.BasinId
	inner join AC.DimProject dp on dp.ProjectId = fps.ProjectID
	inner join dbo.FactProject fpr on fpr.ProjectID = fps.ProjectID
	inner join dbo.DimProject dproj on dproj.ProjectID = fps.ProjectID
where da.AssetSnapshotID = 1