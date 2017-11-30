'''
This file is for testing various snippets of code
It is not use for any production work
'''
import os, json
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.FileUtilities import FileUtilities

tableString = '''{
            "schemaName": "eaa_dev",
            "table": "tcc_cftc",
            "new": "Y",
            "fields": [
                { "name": "SourceSet", "type": "VARCHAR", "size": "10", "isPartitioned": "Y" },
                { "name": "SourceSetDesc", "type": "VARCHAR", "size": "50" },
                { "name": "Market_and_Exchange_Names", "type": "VARCHAR", "size": "100" },
                { "name": "As_of_Date_In_Form_YYMMDD", "type": "INTEGER"},
                { "name": "Report_Date_as_YYYY_MM_DD", "type": "DATE" },
                { "name": "CFTC_Contract_Market_Code", "type": "VARCHAR", "size": "40" },
                { "name": "CFTC_Market_Code", "type": "VARCHAR", "size": "40" },
                { "name": "CFTC_Region_Code", "type": "INTEGER"},
                { "name": "CFTC_Commodity_Code", "type": "VARCHAR", "size": "10" },
                { "name": "Open_Interest_All", "type": "REAL" },
                { "name": "Prod_Merc_Positions_Long_All", "type": "INTEGER"},
                { "name": "Prod_Merc_Positions_Short_All", "type": "INTEGER"},
                { "name": "Swap_Positions_Long_All", "type": "INTEGER"},
                { "name": "Swap__Positions_Short_All", "type": "INTEGER"},
                { "name": "Swap__Positions_Spread_All", "type": "INTEGER"},
                { "name": "M_Money_Positions_Long_All", "type": "INTEGER"},
                { "name": "M_Money_Positions_Short_All", "type": "INTEGER"},
                { "name": "M_Money_Positions_Spread_All", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Long_All", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Short_All", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Spread_All", "type": "INTEGER"},
                { "name": "Tot_Rept_Positions_Long_All", "type": "INTEGER"},
                { "name": "Tot_Rept_Positions_Short_All", "type": "INTEGER"},
                { "name": "NonRept_Positions_Long_All", "type": "INTEGER"},
                { "name": "NonRept_Positions_Short_All", "type": "INTEGER"},
                { "name": "Open_Interest_Old", "type": "INTEGER"},
                { "name": "Prod_Merc_Positions_Long_Old", "type": "INTEGER"},
                { "name": "Prod_Merc_Positions_Short_Old", "type": "INTEGER"},
                { "name": "Swap_Positions_Long_Old", "type": "INTEGER"},
                { "name": "Swap__Positions_Short_Old", "type": "INTEGER"},
                { "name": "Swap__Positions_Spread_Old", "type": "INTEGER"},
                { "name": "M_Money_Positions_Long_Old", "type": "INTEGER"},
                { "name": "M_Money_Positions_Short_Old", "type": "INTEGER"},
                { "name": "M_Money_Positions_Spread_Old", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Long_Old", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Short_Old", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Spread_Old", "type": "INTEGER"},
                { "name": "Tot_Rept_Positions_Long_Old", "type": "INTEGER"},
                { "name": "Tot_Rept_Positions_Short_Old", "type": "INTEGER"},
                { "name": "NonRept_Positions_Long_Old", "type": "INTEGER"},
                { "name": "NonRept_Positions_Short_Old", "type": "INTEGER"},
                { "name": "Open_Interest_Other", "type": "INTEGER"},
                { "name": "Prod_Merc_Positions_Long_Other", "type": "INTEGER"},
                { "name": "Prod_Merc_Positions_Short_Other", "type": "INTEGER"},
                { "name": "Swap_Positions_Long_Other", "type": "INTEGER"},
                { "name": "Swap__Positions_Short_Other", "type": "INTEGER"},
                { "name": "Swap__Positions_Spread_Other", "type": "INTEGER"},
                { "name": "M_Money_Positions_Long_Other", "type": "INTEGER"},
                { "name": "M_Money_Positions_Short_Other", "type": "INTEGER"},
                { "name": "M_Money_Positions_Spread_Other", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Long_Other", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Short_Other", "type": "INTEGER"},
                { "name": "Other_Rept_Positions_Spread_Other", "type": "INTEGER"},
                { "name": "Tot_Rept_Positions_Long_Other", "type": "INTEGER"},
                { "name": "Tot_Rept_Positions_Short_Other", "type": "INTEGER"},
                { "name": "NonRept_Positions_Long_Other", "type": "INTEGER"},
                { "name": "NonRept_Positions_Short_Other", "type": "INTEGER"},
                { "name": "Change_in_Open_Interest_All", "type": "INTEGER"},
                { "name": "Change_in_Prod_Merc_Long_All", "type": "INTEGER"},
                { "name": "Change_in_Prod_Merc_Short_All", "type": "INTEGER"},
                { "name": "Change_in_Swap_Long_All", "type": "INTEGER"},
                { "name": "Change_in_Swap_Short_All", "type": "INTEGER"},
                { "name": "Change_in_Swap_Spread_All", "type": "INTEGER"},
                { "name": "Change_in_M_Money_Long_All", "type": "INTEGER"},
                { "name": "Change_in_M_Money_Short_All", "type": "INTEGER"},
                { "name": "Change_in_M_Money_Spread_All", "type": "INTEGER"},
                { "name": "Change_in_Other_Rept_Long_All", "type": "INTEGER"},
                { "name": "Change_in_Other_Rept_Short_All", "type": "INTEGER"},
                { "name": "Change_in_Other_Rept_Spread_All", "type": "INTEGER"},
                { "name": "Change_in_Tot_Rept_Long_All", "type": "INTEGER"},
                { "name": "Change_in_Tot_Rept_Short_All", "type": "INTEGER"},
                { "name": "Change_in_NonRept_Long_All", "type": "INTEGER"},
                { "name": "Change_in_NonRept_Short_All", "type": "INTEGER"},
                { "name": "Pct_of_Open_Interest_All", "type": "REAL" },
                { "name": "Pct_of_OI_Prod_Merc_Long_All", "type": "REAL" },
                { "name": "Pct_of_OI_Prod_Merc_Short_All", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Long_All", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Short_All", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Spread_All", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Long_All", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Short_All", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Spread_All", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Long_All", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Short_All", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Spread_All", "type": "REAL" },
                { "name": "Pct_of_OI_Tot_Rept_Long_All", "type": "REAL" },
                { "name": "Pct_of_OI_Tot_Rept_Short_All", "type": "REAL" },
                { "name": "Pct_of_OI_NonRept_Long_All", "type": "REAL" },
                { "name": "Pct_of_OI_NonRept_Short_All", "type": "REAL" },
                { "name": "Pct_of_Open_Interest_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Prod_Merc_Long_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Prod_Merc_Short_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Long_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Short_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Spread_Old", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Long_Old", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Short_Old", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Spread_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Long_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Short_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Spread_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Tot_Rept_Long_Old", "type": "REAL" },
                { "name": "Pct_of_OI_Tot_Rept_Short_Old", "type": "REAL" },
                { "name": "Pct_of_OI_NonRept_Long_Old", "type": "REAL" },
                { "name": "Pct_of_OI_NonRept_Short_Old", "type": "REAL" },
                { "name": "Pct_of_Open_Interest_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Prod_Merc_Long_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Prod_Merc_Short_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Long_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Short_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Swap_Spread_Other", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Long_Other", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Short_Other", "type": "REAL" },
                { "name": "Pct_of_OI_M_Money_Spread_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Long_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Short_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Other_Rept_Spread_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Tot_Rept_Long_Other", "type": "REAL" },
                { "name": "Pct_of_OI_Tot_Rept_Short_Other", "type": "REAL" },
                { "name": "Pct_of_OI_NonRept_Long_Other", "type": "REAL" },
                { "name": "Pct_of_OI_NonRept_Short_Other", "type": "REAL" },
                { "name": "Traders_Tot_All", "type": "INTEGER"},
                { "name": "Traders_Prod_Merc_Long_All", "type": "INTEGER"},
                { "name": "Traders_Prod_Merc_Short_All", "type": "INTEGER"},
                { "name": "Traders_Swap_Long_All", "type": "INTEGER"},
                { "name": "Traders_Swap_Short_All", "type": "INTEGER"},
                { "name": "Traders_Swap_Spread_All", "type": "INTEGER"},
                { "name": "Traders_M_Money_Long_All", "type": "INTEGER"},
                { "name": "Traders_M_Money_Short_All", "type": "INTEGER"},
                { "name": "Traders_M_Money_Spread_All", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Long_All", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Short_All", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Spread_All", "type": "INTEGER"},
                { "name": "Traders_Tot_Rept_Long_All", "type": "INTEGER"},
                { "name": "Traders_Tot_Rept_Short_All", "type": "INTEGER"},
                { "name": "Traders_Tot_Old", "type": "INTEGER"},
                { "name": "Traders_Prod_Merc_Long_Old", "type": "INTEGER"},
                { "name": "Traders_Prod_Merc_Short_Old", "type": "INTEGER"},
                { "name": "Traders_Swap_Long_Old", "type": "INTEGER"},
                { "name": "Traders_Swap_Short_Old", "type": "INTEGER"},
                { "name": "Traders_Swap_Spread_Old", "type": "INTEGER"},
                { "name": "Traders_M_Money_Long_Old", "type": "INTEGER"},
                { "name": "Traders_M_Money_Short_Old", "type": "INTEGER"},
                { "name": "Traders_M_Money_Spread_Old", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Long_Old", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Short_Old", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Spread_Old", "type": "INTEGER"},
                { "name": "Traders_Tot_Rept_Long_Old", "type": "INTEGER"},
                { "name": "Traders_Tot_Rept_Short_Old", "type": "INTEGER"},
                { "name": "Traders_Tot_Other", "type": "INTEGER"},
                { "name": "Traders_Prod_Merc_Long_Other", "type": "INTEGER"},
                { "name": "Traders_Prod_Merc_Short_Other", "type": "INTEGER"},
                { "name": "Traders_Swap_Long_Other", "type": "INTEGER"},
                { "name": "Traders_Swap_Short_Other", "type": "INTEGER"},
                { "name": "Traders_Swap_Spread_Other", "type": "INTEGER"},
                { "name": "Traders_M_Money_Long_Other", "type": "INTEGER"},
                { "name": "Traders_M_Money_Short_Other", "type": "INTEGER"},
                { "name": "Traders_M_Money_Spread_Other", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Long_Other", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Short_Other", "type": "INTEGER"},
                { "name": "Traders_Other_Rept_Spread_Other", "type": "INTEGER"},
                { "name": "Traders_Tot_Rept_Long_Other", "type": "INTEGER"},
                { "name": "Traders_Tot_Rept_Short_Other", "type": "INTEGER"},
                { "name": "Conc_Gross_LE_4_TDR_Long_All", "type": "REAL" },
                { "name": "Conc_Gross_LE_4_TDR_Short_All", "type": "REAL" },
                { "name": "Conc_Gross_LE_8_TDR_Long_All", "type": "REAL" },
                { "name": "Conc_Gross_LE_8_TDR_Short_All", "type": "REAL" },
                { "name": "Conc_Net_LE_4_TDR_Long_All", "type": "REAL" },
                { "name": "Conc_Net_LE_4_TDR_Short_All", "type": "REAL" },
                { "name": "Conc_Net_LE_8_TDR_Long_All", "type": "REAL" },
                { "name": "Conc_Net_LE_8_TDR_Short_All", "type": "REAL" },
                { "name": "Conc_Gross_LE_4_TDR_Long_Old", "type": "REAL" },
                { "name": "Conc_Gross_LE_4_TDR_Short_Old", "type": "REAL" },
                { "name": "Conc_Gross_LE_8_TDR_Long_Old", "type": "REAL" },
                { "name": "Conc_Gross_LE_8_TDR_Short_Old", "type": "REAL" },
                { "name": "Conc_Net_LE_4_TDR_Long_Old", "type": "REAL" },
                { "name": "Conc_Net_LE_4_TDR_Short_Old", "type": "REAL" },
                { "name": "Conc_Net_LE_8_TDR_Long_Old", "type": "REAL" },
                { "name": "Conc_Net_LE_8_TDR_Short_Old", "type": "REAL" },
                { "name": "Conc_Gross_LE_4_TDR_Long_Other", "type": "REAL" },
                { "name": "Conc_Gross_LE_4_TDR_Short_Other", "type": "REAL" },
                { "name": "Conc_Gross_LE_8_TDR_Long_Other", "type": "REAL" },
                { "name": "Conc_Gross_LE_8_TDR_Short_Other", "type": "REAL" },
                { "name": "Conc_Net_LE_4_TDR_Long_Other", "type": "REAL" },
                { "name": "Conc_Net_LE_4_TDR_Short_Other", "type": "REAL" },
                { "name": "Conc_Net_LE_8_TDR_Long_Other", "type": "REAL" },
                { "name": "Conc_Net_LE_8_TDR_Short_Other", "type": "REAL" },
                { "name": "Contract_Units", "type": "VARCHAR", "size": "200" },
                { "name": "CFTC_Contract_Market_Code_Quotes", "type": "VARCHAR", "size": "30" },
                { "name": "CFTC_Market_Code_Quotes", "type": "VARCHAR", "size": "30" },
                { "name": "CFTC_Commodity_Code_Quotes", "type": "VARCHAR", "size": "30" },
                { "name": "CFTC_SubGroup_Code", "type": "VARCHAR", "size": "30" },
                { "name": "FutOnly_or_Combined", "type": "VARCHAR", "size": "40" }
            ]           
        }'''
   
table = json.loads(tableString)
logger = FileUtilities.CreateLogger("log", 10)

os.environ["SPARK_HOME"] = "C:/WorkSpaceEclipse36/SparkWindows/spark"
os.environ["HADOOP_HOME"] = "C:/WorkSpaceEclipse36/SparkWindows/hadoop"

sc, sqlContext = SparkUtilities.CreateSparkContext(logger)

samplejson = '''{
    "fields": [
        {"metadata": {}, "nullable": true, "name": "sourceset", "type": "string"}, 
        {"metadata": {}, "nullable": true, "name": "sourcesetdesc", "type": "string"}, 
        {"metadata": {}, "nullable": true, "name": "market_and_exchange_names", "type": "string"}, 
        {"metadata": {}, "nullable": true, "name": "as_of_date_in_form_yymmdd", "type": "integer"}, 
        {"metadata": {}, "nullable": true, "name": "report_date_as_yyyy_mm_dd", "type": "string"}
    ],
    "type": "struct"
}'''
#schemaJson = json.loads(samplejson)
#from pyspark.sql.types import StructType#@UnresolvedImport

#schema = StructType.fromJson(schemaJson)

schema = SparkUtilities.BuildSparkSchema(table)

#print(schema)
localTxtFilepath = "C:/tmp/testfiles/COTHist2011.csv"



df = (sqlContext.read
         .format("csv")
         .option("header", "true")
         .option("delimiter", ",")
         .option("ignoreTrailingWhiteSpace", "true")
         .option("ignoreLeadingWhiteSpace", "true")
         .schema(schema)
         .load(localTxtFilepath)
         )
df.printSchema()

df.show()
sc.stop()
