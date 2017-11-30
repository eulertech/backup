	SELECT 
		a.ProductID, 
		pd.DisplayText as product, 
		a.locationID, 
		lc.DisplayText as location, 
		a.CategoryID, 
		c.DisplayText as category, 
		a.year, 
		a.Value
	FROM PeriodValues a
	JOIN product pd 
	  ON pd.ProductID = a.ProductID 
	JOIN Location lc 
		ON (lc.LocationID = a.LocationID
		AND lc.LocationTypeID != '2'
		AND lc.locationID not in ('USR','DDR'))
	JOIN Category c 
		ON c.CategoryID =  a.CategoryID
	WHERE a.CategoryID in ('19', '10', '12', '15')
		AND a.SubcategoryID = '0'