/*
	Type Well Annual Data
*/
select	fwp.WellID, 
		da.AssetName,
		db.BasinName,
		dp.ProjectName,
		dc.CountryName,
		dps.PriceScenarioDescription,
		fwp.YearNumber,
		fwp.WellCapitalAbandonment, 
		fwp.WellCapitalTotal, 
		fwp.WellDrillingCostTotal, 
		fwp.WellGasPrice, 
		fwp.WellGrossGasRevenue, 
		fwp.WellGrossLiquidsRevenue, 
		fwp.WellGrossOilRevenue, 
		fwp.WellGrossTotalRevenue, 
		fwp.WellIncomeTax,
		fwp.WellOilPrice, 
		fwp.WellOperationalCostTotal, 
		fwp.WellOperationalCostTotalIncludingBonusesFees, 
		fwp.WellOperationalCostVariable, 
		fwp.WellProductionCO2Emissions,
		fwp.WellPercentageRemainingReserves,
		fwp.WellProductionOilVolume,
		fwp.WellProductionGasRate,
		fwp.WellProductionOilRate,
		fwp.WellProfitGas,
		fwp.WellProfitOil, 
		fwp.WellTotalCost, 
		fwp.WellTotalCostRecovery,
		fwp.WellProductionOtherLiquidsVolume
from AC.FactWellPeriodic fwp
	inner join AC.DimBasin db on db.BasinId = fwp.BasinId
	inner join AC.DimCountry dc on dc.CountryId = fwp.CountryID
	inner join AC.DimPriceScenario dps on dps.PriceScenarioID = fwp.PriceScenarioID
	inner join AC.DimProject dp on dp.ProjectId = fwp.ProjectID
	inner join AC.DimAsset da on da.AssetId = fwp.AssetID
where fwp.SnapshotID = 1