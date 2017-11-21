#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

library(shiny)
library(reshape2)
library(ggplot2)
library(dygraphs)
library(xts)
source("global.R")
# Define server logic required to draw a histogram
shinyServer(function(input, output) {
  rv <- reactiveValues()
  rv$data <- NULL
  rv$dataxts <- NULL
  rv$startDate <- NULL
  rv$endDate <- NULL
  observe({  # will 'obseve' the button press
    if(input$go) {
      print("rename x column name to 'date'")
      colnames(data)[which(names(data)==input$xvarID)] <- "date"
      data$date <- as.Date(data$date)
      startDate <- data$date[1]
      endDate <- data$date[length(data$date)]
      #      data$date <- data[,which(names(data)==input$xvarID)]
      # get selected variable name list
      print("get selected variable from selectizeInput")
      selectedVars <- isolate(input$selectVar2PlotID)
      if(length(selectedVars)<1) {
        #stop ("Please select at lease one variable!!")
      }
      # subset data using the date range and selected variables list
      data2use <- subset(data,date>=as.Date(as.character(input$dateRangeID[1])) 
                         & date <= as.Date(as.character(input$dateRangeID[2])),
                         select = c('date',selectedVars))
      # Normalize data2use if minmax chosen
      if(input$normMethodID == 'minmax') {
        print('minmax normalization selected!')
        n <- dim(data2use)[2] 
        data2use[selectedVars] <- lapply(data2use[selectedVars], function(x) (x - min(x))/(max(x)-min(x)) )
      }
      data2use[,1] <- as.Date(data2use[,1])
      rv$data <- data2use
      rv$dataxts <- xts(subset(data2use, select = selectedVars), order.by = data2use[,1])
      rv$startDate <- startDate
      rv$endDate <- endDate
      return(rv)
    }
    
  })  
  
  #xymelt <-eventReactive(input$go, {melt(rv$data,id.vars = 'date')}) # for using with ggplot2 
  #observeEvent(input$go, {write.csv(rv$data, file = './www/rvdata.csv')})
  #observeEvent(input$go, {write.csv(xymelt(), file = './www/xymelt.csv')})
  
  #    output$textID <- renderText(str(rv$data))
  #output$textID <- renderText(paste0('debug',as.character(rv$startDate)))
  output$dateID <- renderUI(dateRangeInput('dateRangeID',paste0("Date Range:",rv$startDate,' to ',rv$endDate), start = rv$startDate, 
                                           end = rv$endDate, min = rv$startDate, max = rv$endDate))
  #output$plot <- renderPlot({
  #  ggplot(xymelt(), aes(x = date, y = value, color=variable)) + geom_line() +
  #    geom_point(data = xymelt(),aes(x = date, y = value, color=variable))
  #})
  # dygraph plots
  output$dygraph <- renderDygraph({
    if(!input$go){
      return
    }
    selectedVars <- isolate(input$selectVar2PlotID)
    tryCatch(
      dygraph(rv$dataxts,main = 'Interactive Time Series Viz') %>%
        dyRangeSelector() %>% 
        dyRoller(rollPeriod=1) %>%
        dyHighlight(highlightSeriesOpts = list(strokeWidth = 3),
                    highlightCircleSize = 5,
                    highlightSeriesBackgroundAlpha = 0.3,
                    hideOnMouseOut = FALSE)
    )
  }) 
  
  output$table <- renderDataTable(rv$data)
  output$lookupTable <- renderDataTable(lookupData)
})
