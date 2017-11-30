/*
	Conventional and Unconventional Annual Data (Asset Level)
*/
select	da.AssetName,
		db.BasinName,
		dps.PriceScenarioDescription,
		di.InflationValue,
		dct.ConsolidationType,
		dc.CountryName,
		fap.YearID,
		da.assetisunconventional,
		fap.PercentageRemainingReserves,
		fap.CapitalAbandonment,
		fap.CapitalTotalExcludingAbandonment,
		fap.CapitalDevelopmentTotal,
		fap.CumulativeProductionGasEquivalent,
		fap.CumulativeProductionOilEquivalent,
		fap.DrillingCostTotal,
		fap.GasPrice,
		fap.ProductionGasRate,
		fap.ProductionOilRate,
		fap.GrossGasRevenue,
		fap.GrossLiquidsRevenue,
		fap.GrossOilRevenue,
		fap.GrossTotalRevenue,
		fap.IncomeTax,
		fap.OilPrice,
		fap.OperationalCostTotal,
		fap.OperationalCostTotalIncludingBonusesFees,
		fap.OperationalCostVariable,
		fap.ProductionCO2Emissions,
		fap.ProductionOilVolume,
		fap.ProductionGasVolume,
		fap.ProfitGas,
		fap.ProfitOil,
		fap.TotalCost,
		fap.TotalCostRecovery
from AC.V_FactAssetPeriodic fap
	inner join AC.DimAsset da on da.AssetId = fap.AssetID
	inner join AC.DimBasin db on db.BasinId = fap.BasinId
	inner join AC.DimCountry dc on dc.CountryId = fap.CountryID
	inner join AC.DimPriceScenario dps on dps.PriceScenarioID = fap.PriceScenarioID
	inner join AC.DimInflation di on di.InflationId = fap.InflationId
	inner join AC.DimConsolidationType dct on dct.ConsolidationID = fap.ConsolidationTypeID
where fap.SnapshotID = 1