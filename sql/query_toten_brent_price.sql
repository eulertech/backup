-- @WbResult totem
SELECT valuationdate,
       clientid,
       name,
       totemgroup,
       units,
       pricingtime,
       period,
       startdate,
       enddate,
       totemtype,
       price,
       consensusprice,
       compositeprice,
       pricerange,
       contributors,
       pricestddev, 
       strike,
       vol,
       reconstitutedforward,
       consensusvol,
       compositevol,
       volrange,
       expirydate,
       volstddev
FROM eaa_prod.totem 
WHERE period = 'Month' and totemgroup = 'Crude Oil' and name = 'BRENT'
ORDER BY startdate desc 
 ;
