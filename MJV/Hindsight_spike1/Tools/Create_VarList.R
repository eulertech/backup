# Creates or refresh the "Var List of Interest" used for Shiny/Hinsight spike


library(AACloudTools)
library(AASpectre)

origWd <- getwd()
setwd("./Personal/MJV/Hindsight_spike1/")

vl <- New_SpectreVarList(id="TSVIZ_Demo_f1c3ad28",
                         projectName="Demo TimeSeries Viz", listType="W",
                         listName="France_Italy_Industrial",
                         shortDescr="French and Italian Industrial series",
                         longDescr="",
                         recipe=NULL)

sql <- c("SELECT hindsight_prod.MnemoToSyntactic(mnemonic) AS VarId, ",
         "RANK() OVER (ORDER BY shortlabel) AS zeSeqNo, 321 AS vWhatever, series_id,  shortlabel",
         "FROM hindsight_prod.series_attributes ",
         "WHERE frequency = 'ANNL' AND shortlabel ~ '(France|Italy)' AND shortlabel LIKE '%Industrial%'")

# ~ 500 variables
nbVars <- vl$ImportListFromSql(sql,
                               inColToLovColMap=c(VarId="VarId", SeqNr="zeSeqNo", IntVar1="series_id", NumVar2="vWhatever", StrVar4="shortlabel"))


setwd(origWd)
