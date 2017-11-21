#
# This is the user-interface definition of a Shiny web application. You can
# run the application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

library(shiny)
library(xts)
library(dygraphs)

# debug time only...
#origWd <- getwd()
#setwd("./Personal/MJV/Hindsight_spike1/")

library(AACloudTools)
library(AASpectre)

# *** If we want some logging...
#Logger <- AALogger()
#Logger$SetLogFile(paste0("Spike1", ".log"), logNr=2,  overwrite=TRUE)
#Logger$SetLogLevel("DBG", logNr=2)
#Logger$SetLogLevel("DBG", logNr=1) # dbg time only...


source("global.R")

# Define UI for application that draws a histogram
shinyUI(fluidPage(
  
  # Application title
  titlePanel("Time Series Interactive Visualization Demo (Dr. Liang Kuang)"),
  
  # Sidebar with a slider input for number of bins 
  sidebarLayout(
    sidebarPanel(
      #dateRangeInput('dateRangeID',paste0("Date Range:",startDate,' to ',endDate), start = rv$startDate, 
      #               end = endDate, min = startDate, max = endDate),
      uiOutput('dateID'),
      selectInput('xvarID',label='Select x-axis variable(Must be DateTime Variable)',choices = features,selected = 'date'),
      selectizeInput(
        'selectVar2PlotID', label = 'Select variables to explore (max =5)', choices = features2include ,
        options = list(maxItems=5,placeholder = 'select a feature variable')
      ),
      radioButtons("normMethodID","Data Transform Method",
                   c("Normalize to [0,1]" = "minmax",
                     "Raw Data" = "raw")),
      actionButton("go","Update"),
      #verbatimTextOutput('textID'),
      tags$div(
        HTML("<p> Instructions to use the app: </p> 
                  <ul>
                    <li> 0. Search the variable ID in the variableID lookup table on the right</li>
                    <li> 1. Choose the x-axis variable in date & time format</li>
                    <li> 2. Click 'update' to get the available datetime range in the data using the selected x-variable </li>
                    <li> 3. Select date range using the date selector on top</li>
                    <li> 4. Select/Type multiple variables to visualize by typing or clicking (autofill enabled)</li>
                    <li> 5. Click 'update' to show the table and plot.</li>
                    <li> 6. You can also use the seach box to search variables after 'update'</li>
                  </ul>
             ")
      )),
    
    
    # Show a plot of the generated distribution
    mainPanel(
      img(src='logo.png',alt='IHS Markit', height=30, width=100,align = 'right'),
      tabsetPanel(
        tabPanel("Table",dataTableOutput("table")),
        tabPanel("Variable ID Lookup", dataTableOutput("lookupTable")),
        tabPanel("Dygraph",dygraphOutput("dygraph"))
        #tabPanel("Plot",plotOutput("plot"))
      )
    )
  )
))
