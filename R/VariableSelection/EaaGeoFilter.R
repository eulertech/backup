# Geo Filter allows you to filter a dataframe by geography. It requires a dataframe and outputs a geofiltered dataframe
# Region filters are "All" "Middle East" "Asia" "Latin America" "Europe"  "World Aggregate" "North America" "Africa"   
# Input: dataframe with columns as variables
# Output: dataframe with columns as variables, trimmed
# Author: Lou Zhang

EaaGeoFilter <- function(df, region = 'all', producer = TRUE, consumer = TRUE) {
  
  df <- as.data.frame(df)
  
  geocodes <- AACloudTools::SqlToDf('SELECT * FROM eaa_analysis.prod_oil_geo_codes')
  reftable <- AACloudTools::SqlToDf("SELECT * FROM eaa_prod.eaa_attributes WHERE source like 'Connect' and frequency like 'M'")
  geolist <- NULL
  
  regionIn <- region
  
  if(regionIn == 'all') {
    regionIn <- unique(geocodes$region)
  }
  
  if(producer == TRUE) {
    categoryIn <- 'Producer'
  }
  
  if(consumer == TRUE) {
    categoryIn <- 'Consumer'
  }
  
  if(producer == TRUE & consumer == TRUE) {
    categoryIn <- unique(geocodes$category)
  }
  
  code <- subset(geocodes, region %in% regionIn & category %in% categoryIn)$idds_code
  reftableConsidered <- reftable[reftable$geo %in% code,]
  
  dfGeoFiltered <- df[,colnames(df) %in% reftableConsidered$name]

  return(dfGeoFiltered)
}

