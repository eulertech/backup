###############################################################################
##
##  Spike_Bravo : a working demo of a Shiny app which can be used to select
##                a list of Time Series by search/browsing in a "catalog" of
##                time series.
##
## Author: Marc Veillet
##   This is loosely based on 
##      https://github.com/AntoineGuillot2/ButtonsInDataTable, aka
##      http://enhancedatascience.com/2017/03/01/three-r-shiny-tricks-to-make-your-shiny-app-shines-33-buttons-to-delete-edit-and-compare-datatable-rows/
##   Essentially:
##      - Adding a column with hmtl snippet that produces buttons or other controls
##         (typically with an ID such that the underlying data / action can be parsed)
##      - Adding a JavaScript snippet which produces some Event when the buttons/controls
##        are clicked-upon
##      - An event handler that performs the desired action as parsed from the input
##        (or implicit as it may be)
##
###############################################################################

# Idiosyncrasies, bugs and ideas for future work...
#  - need to figure out how to make "Description" column wider (from the start)
#  - use css to make font of "Description" smaller
#  - find a way to make the formating of numeric values work
#        (render="$.fn.dataTable.render.number(',',',', 3 )"  doesn't work; had to format in the data source itself)
#  - Introduce a Master-Detail view of sorts.
#


# For debugging only...
# options(shiny.reactlog=TRUE)
# remember to put it back to FALSE when done !


library("shiny")
library("stringr")
library("DT")
library("AACloudTools")
library("AASpectre")

ns <- function(x) {x}

ui <- fluidPage(
  sidebarLayout(
    sidebarPanel(
      dataTableOutput("tblSelectedVars"),
      
      br(),br(),
      p(strong("Dbg Info:")),
      textOutput("dbgInfo")
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Catalog of Variables", br(),
                 dataTableOutput("varCatalog")
        ),
        tabPanel("Tab 2", br(),
                 p(strong("Tab Deux"))
                 # dataTableOutput("tblSelectedVars")
        ),
        tabPanel("Tab 3", br(),
                 p(strong("Tab Drei"))
        )
      )
    )
  ),
  # Javascript snippet which sends "lastClick" events
  tags$script(paste0(
    "$(document).on('click', '#varCatalog button', function () { ",
       sprintf("Shiny.onInputChange('%s', this.id); ", ns("lastClickId")),
       sprintf("Shiny.onInputChange('%s', Math.random()) ", ns("lastClick")),
    "});")),
  
  # tags$script("$(document).on('click', '#varCatalog button', function () {
  #             Shiny.onInputChange('lastClickId',this.id);
  #             Shiny.onInputChange('lastClick', Math.random())
  #             });"),
  
  tags$script("$(document).on('click', '#tblSelectedVars button', function () {
              Shiny.onInputChange('lastClickId',this.id);
              Shiny.onInputChange('lastClick', Math.random())
              });")
  
  
)


# Initialize container with all the global variables
GetGlobals <- function() {
  retVal <- list()
  retVal$SELECT_META_ATTRIBUTES <- c("mnemonic", "cluster", "blendedscore", "startdate", "enddate", "shortlabel")
     # <- c("mnemonic", "cluster", "blendedscore", "startdate", "enddate", "shortlabel")
  
  ConfigureAWS("../Config/config.json")
  cn <- GetRedshiftConnection() 
  retVal$fullCatalog <- dbGetQuery(cn,
                                   "SELECT variableid as mnemonic, *,  RANK() OVER (ORDER BY variableid) AS SeqNr  FROM eaa_analysis.feature_selection_matrix_csv ORDER BY  RANK() OVER (ORDER BY variableid)")
  plusBtns  <-  paste0("<div><button type='button' class='btn btn-default btn-xs' id=add_", retVal$fullCatalog$mnemonic, ">+</button>")
  minusBtns <-  paste0("<button type='button' class='btn btn-default btn-xs' id=rmv_", retVal$fullCatalog$mnemonic, ">-</button></div>")
  selColumn <- paste(plusBtns, minusBtns, sep="&nbsp;&nbsp;&nbsp;")
  retVal$varCatalog <- cbind(retVal$fullCatalog[, retVal$SELECT_META_ATTRIBUTES], 
                             data.frame(SelectColumn=selColumn))
  
  retVal$varCatalog$blendedscore <- round(retVal$varCatalog$blendedscore, 3)   # @@@ kludge there's gotta be a better way...
  retVal$varCatalog$cluster <- as.factor(retVal$varCatalog$cluster)    # In truth cluster is more of a factor than an integer; this avoids the slider to select the [integer!] values
  
  retVal$varCatalogDatatableOptions  <- list(
    autoWidth=FALSE,
    columns=list(
      list(title="row num"),
      list(title="Mnemonic"),
      list(title="Cluster"),
      list(title="Blend Score"),
      list(title="Start Date"),
      list(title="End Date"),
      list(title="Description", width="70%"),
      list(title="Select / Unselect", searchable=FALSE, orderable=FALSE)
    )
    
    ,
    searchCols=list(
      NULL, # list(regex=TRUE),     # not really necessary.
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    )
  )
  
  retVal
}

gbl <- GetGlobals()

server <- function(input, output, session) {
  dbgInfo <- reactiveVal()
  selectedVars <- reactiveVal()
  
  output$varCatalog <- DT::renderDataTable(
    datatable(gbl$varCatalog, 
              options=gbl$varCatalogDatatableOptions, escape=FALSE, filter="top", selection="none"))
  
  output$tblSelectedVars <- DT::renderDataTable( {
    if (is.null(selectedVars()) || length(selectedVars()) == 0) {
      return(NULL)  # @@@ see to improve....
    }
    dtOptions <- list(
      searching=FALSE, paging=FALSE, info=FALSE,
      columns=list(
        list(title="Remove"),
        list(title="Mnemonic"),
        list(title="Description")
      )
    )
    
    d.t <- gbl$varCatalog[gbl$varCatalog$mnemonic %in% selectedVars(), c("mnemonic", "shortlabel")]
    minusBtns <-  paste0("<button type='button' class='btn btn-default btn-xs' id=rmv_", d.t$mnemonic, ">-</button></div>")
    datatable(cbind(data.frame(Remove=minusBtns, d.t)), 
              options=dtOptions,
              class="stripe hover",
              escape=FALSE, rownames=FALSE, filter="none", selection="single", autoHideNavigation=TRUE)
  })
  
  output$dbgInfo <- renderPrint({
    dbgInfo()
    selectedVars()
  })
  
  # lastClick is the event generated by javascript snippet when user clicks on buttons on the varCatalog table.
  observeEvent(input$lastClick, {
    dbgInfo(input$lastClickId)
    
    parsedId <- str_match(input$lastClickId, "^(add|rmv)_(.+)$")
    # Ignore button clicks other than on add_ and rmv_ buttons
    if (ncol(parsedId) != 3 && !is.na(parsedId[2]))
      return()
    varId <- parsedId[3]
    sv <- selectedVars()
    
    if (parsedId[2] == "add") {
      selectedVars(union(sv, varId))
    } else {
      selectedVars(setdiff(sv, varId))
    }
  })
}


shinyApp(ui, server)