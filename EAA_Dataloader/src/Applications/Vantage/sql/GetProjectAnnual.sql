/*
	Conventional and Unconventional Annual Data (Project Level)
*/
select	da.AssetName,
		db.BasinName,
		dp.ProjectName,
		dps.PriceScenarioDescription,
		di.InflationValue,
		dct.ConsolidationType,
		dc.CountryName,
		fpp.YearID,
		da.assetisunconventional,
		fpp.PercentageRemainingReserves,
		fpp.CapitalAbandonment,
		fpp.CapitalTotalExcludingAbandonment,
		fpp.CapitalDevelopmentTotal,
		fpp.CumulativeProductionGasEquivalent,
		fpp.CumulativeProductionOilEquivalent,
		fpp.DrillingCostTotal,
		fpp.GasPrice,
		fpp.ProductionGasRate,
		fpp.ProductionOilRate,
		fpp.GrossGasRevenue,
		fpp.GrossLiquidsRevenue,
		fpp.GrossOilRevenue,
		fpp.GrossTotalRevenue,
		fpp.IncomeTax,
		fpp.OilPrice,
		fpp.OperationalCostTotal,
		fpp.OperationalCostTotalIncludingBonusesFees,
		fpp.OperationalCostVariable,
		fpp.ProductionCO2Emissions,
		fpp.ProductionOilVolume,
		fpp.ProductionGasVolume,
		fpp.ProfitGas,
		fpp.ProfitOil,
		fpp.TotalCost,
		fpp.TotalCostRecovery
from AC.V_FactProjectPeriodic fpp
	inner join AC.DimAsset da on da.AssetId = fpp.AssetID
	inner join AC.DimBasin db on db.BasinId = fpp.BasinId
	inner join AC.DimCountry dc on dc.CountryId = fpp.CountryID
	inner join AC.DimProject dp on dp.ProjectId = fpp.ProjectID
	inner join AC.DimPriceScenario dps on dps.PriceScenarioID = fpp.PriceScenarioID
	inner join AC.DimInflation di on di.InflationId = fpp.InflationId
	inner join AC.DimConsolidationType dct on dct.ConsolidationID = fpp.ConsolidationTypeID
where fpp.SnapshotID = 1