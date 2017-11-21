############################################
# Unit test for Granger Causality Test     
# Author: Liang Kuang                      
############################################

library(fUnitRoots)
library(urca)
library(vars)
library(aod)
library(zoo)
library(tseries)
source('TY_GNC_test.R')
source('generic_GNC_test.R')

#Load data
cof <- read.csv("http://www.christophpfeiffer.org/app/download/6938079586/coffee_data.csv", header=T,sep=";")
names(cof)
#Adjust Date format
cof["Date"]<-paste(sub("M","-",cof$Date),"-01",sep="")

#Visualize
plot(as.Date(cof$Date),cof$Arabica,type="l",col="black",lwd=2)
lines(as.Date(cof$Date),cof$Robusta,col="blue",lty=2,lwd=1)
legend("topleft",c("Arabica","Robusta"),col=c("black","blue"),lty=c(1,2),lwd=c(2,1),bty="n")

#Possible structural break in 1970s. Therefore only values from 1976:01 onwards are regarded
cof1<-cof[193:615,]

#Visualize
plot(as.Date(cof1$Date),cof1$Arabica,type="l",col="black",lwd=2,ylim=range(cof1$Robusta))
lines(as.Date(cof1$Date),cof1$Robusta,col="blue",lty=2,lwd=1)
legend("topright",c("Arabica","Robusta"),col=c("black","blue"),lty=c(1,2),lwd=c(2,1),bty="n")

cof2 <- subset(cof1,select = c('Arabica','Robusta'))
p_val <- TY_GNC_test(cof2)

p_val2 <- generic_GNC_test(cof2)
