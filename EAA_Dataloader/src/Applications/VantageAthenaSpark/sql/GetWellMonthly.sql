/*
	Type Well Monthly Data
*/
select	da.AssetName,
		dp.ProjectName,
		dmn.MonthNumber,
		dmn.MonthQuarter,
		dmn.MonthYear,
		fwpm.MonthlyWellProductionGasVolume,
		fwpm.MonthlyWellProductionOilVolume
from AC.V_FactWellPeriodicMonthly fwpm
	inner join AC.DimMonthNumber dmn on dmn.MonthNumber = fwpm.MonthNumber
	inner join AC.DimProject dp on dp.ProjectId = fwpm.ProjectID
	inner join AC.DimAsset da on da.AssetId = fwpm.FieldID
where fwpm.SnapshotID = 1