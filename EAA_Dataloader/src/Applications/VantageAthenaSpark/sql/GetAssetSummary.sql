/*
	Conventional and Unconventional Summary Data (Asset Level)
*/
select	da.AssetName, 
		db.BasinName, 
		dps.PriceScenarioDescription, 
		di.InflationValue, 
		dc.CountryName, 
		da.AssetIsUnconventional,
		fas.CapitalAbandonment,
		fas.CapitalDevelopmentTotal,
		fas.CapitalTotalExcludingAbandonment,
		fas.GrossTotalRevenue,
		fas.IncomeTax,
		fas.ProductionCO2Emissions,
		fas.TotalCost,
		das.AssetCO2Content,
		fas.OilRecoverableReserves,
		fas.GasPointForwardBreakEvenPrice,
		fas.InternalRateOfReturn,
		fas.TotalAbandonmentCapitalPerGasEquivalent,
		fas.TotalAbandonmentCapitalPerOilEquivalent,
		fas.TotalCostPerGasEquivalent,
		fas.TotalCostPerOilEquivalent,
		fas.TotalDrillingDevelopmentCapitalPerOilEquivalent,
		fas.TotalDrillingDevelopmentCapitalPerGasEquivalent,
		fas.TotalExplorationCapitalPerOilEquivalent,
		fas.TotalExplorationCapitalPerGasEquivalent,
		fas.TotalOpexPerGasEquivalent,
		fas.TotalOpexPerOilEquivalent,
		da.AssetNumberOfProjects,
		fas.DevelopmentCostPerGasEquivalent,
		fas.DevelopmentCostPerOilEquivalent
from AC.V_FactAssetSummary fas
	inner join AC.DimAsset da on da.AssetId = fas.AssetID
	inner join AC.DimPriceScenario dps on dps.PriceScenarioID = fas.PriceScenarioID
	inner join AC.DimInflation di on di.InflationId = fas.InflationId
	inner join AC.DimCountry dc on dc.CountryId = fas.CountryID
	inner join AC.DimBasin db on db.BasinId = fas.BasinId
	inner join AC.FactAsset das on das.AssetID = fas.AssetId 
								and das.BasinID = fas.BasinID 
								and das.SnapshotID = fas.SnapshotID
								and das.CountryID = fas.CountryID
where da.AssetSnapshotID = 1