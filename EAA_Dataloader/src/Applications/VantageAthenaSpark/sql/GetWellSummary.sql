/*
	Type Well Summary Data
*/
select	fwsi.WellTypeID, 
		da.AssetName,
		db.BasinName,
		dp.ProjectName,
		dc.CountryName,
		di.InflationValue,
		dps.PriceScenarioDescription,
		fwsi.WellCapitalAbandonment,
		fwsi.WellCapitalTotal,
		fwsi.WellDrillingCostPerOilEquivalent,
		fwsi.WellDrillingCostPerGasEquivalent,
		fwsi.WellDrillingCostTotal,
		fwsi.WellFAndDPerGasEquivalent,
		fwsi.WellFAndDPerOilEquivalent,
		fwsi.WellGasPrice,
		fwsi.WellGrossGasRevenue,
		fwsi.WellGrossLiquidsRevenue,
		fwsi.WellGrossOilRevenue,
		fwsi.WellGrossTotalRevenue,
		fwsi.WellIncomeTax,
		fwsi.WellInternalRateOfReturn,
		fwsi.WellOilPrice,
		fwsi.WellOperationalCostTotal,
		fwsi.WellOperationalCostTotalIncludingBonusesFees,
		fwsi.WellOperationalCostVariable,
		fwsi.WellProfitGas,
		fwsi.WellProfitOil,
		fwsi.WellTotalCost,
		fwsi.WellTotalCostPerGasEquivalent,
		fwsi.WellTotalCostPerOilEquivalent,
		fwsi.WellTotalCostRecovery,
		fwsi.WellTotalOpexPerOilEquivalent,
		fwsi.WellTotalOpexPerGasEquivalent,
		fws.WellProductionCO2Emissions,
		fws.WellProductionOilVolume,
		da.AssetNumberOfProjects,
		fw.WellGasBreakEvenPrice,
		fw.WellOilBreakEvenPrice,
		fws.WellProductionOtherLiquidsVolume,
		fw.WellTransLiquidsPerOilEquivalent
from AC.V_FactWellSummaryInflation fwsi
	inner join AC.DimBasin db on db.BasinId = fwsi.BasinId
	inner join AC.DimCountry dc on dc.CountryId = fwsi.CountryID
	inner join AC.DimPriceScenario dps on dps.PriceScenarioID = fwsi.PriceScenarioID
	inner join AC.DimInflation di on di.InflationId = fwsi.InflationId
	inner join AC.DimProject dp on dp.ProjectId = fwsi.ProjectID
	inner join AC.DimAsset da on da.AssetId = fwsi.FieldID
	inner join AC.FactWellSummary fws on fws.WellID = fwsi.WellTypeID 
									and fws.PriceScenarioID = fwsi.PriceScenarioID
	inner join AC.FactWell fw on fw.WellID = fwsi.WellTypeID
where fwsi.SnapshotID = 1