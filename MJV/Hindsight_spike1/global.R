data <- data.frame(
  AASpectre::SpectreGetData(varList="TSVIZ_Demo_f1c3ad28", frequency="ANNL",
                            dateFrom="1999-01-01", dateTo="2021-01-01") 
) #read.csv('./www/data.csv')
feature2exclude <- 'date'
features <- names(data)
features2include <- features[!(features %in% feature2exclude)]

# fast way:
vl <- New_SpectreVarList(id="TSVIZ_Demo_f1c3ad28")
lookupData <- vl$Lov[, c("varid", "strvar4")]
colnames(lookupData) <- c("id", "description")
vl <- NULL

# slow way:
#  mnemos <- paste0("hindsight_prod.SyntacticToMnemo('", features2include, "')",  collapse=", ")
#  lookupData <- AASpectre::SpectreGetVarInfo(varIds=NULL, sqlCondition=sprintf("mnemonic in (%s)", mnemos),
#                                             infoFields=c("mnemonic AS id", "shortlabel AS description"))      #read.csv('./www/variable_lookup.csv')

