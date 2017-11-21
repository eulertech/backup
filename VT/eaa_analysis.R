
############################################################
#
# Analysis of the data
#
# Author: Valentin Todorov
#
############################################################

library(TTR)
library(quantmod)
library(nnet)
library(ggplot2)

# Read in the data
dataDf <- read.csv("C:/GitRepos/EAA_Analytics/Data/BrentPrice.csv")

# Transform data
colnames(dataDf) <- c("date", "BrentPrice")
dataDf <- data.frame(dataDf,
                     dateFormated = as.Date(dataDf$date, format = "%m/%d/%Y"))

# Define the frequency of the series
yVar <- ts(dataDf$BrentPrice, frequency = 12)

# Decompose the price and plot
yVar1 <- decompose(yVar)
plot(yVar1)


## Histogram of distribution of price change
dataDf$yVarLag <- Lag(dataDf$BrentPrice, 1)[,1]
dataDf$yVarReturn <- (dataDf$BrentPrice/dataDf$yVarLag - 1)

# Plot histogram of returns
hist(dataDf$yVarReturn, breaks = 40,
     ylab = "Brent - 1 month return",
     main = "Distribution of Brent returns")


# Discretize the data
categories <- c(-1, -0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.2, 1)
dataDf$yVarReturnCat <- cut(dataDf$yVarReturn,
                            breaks = categories)

dataDf$yVarReturnCatLabels <- as.integer(cut(dataDf$yVarReturn,
                                             breaks = categories,
                                             labels = c(1:(length(categories) - 1))))

# Replace NA with last category
dataDf$yVarReturnCatLabels[is.na(dataDf$yVarReturnCatLabels)] <- length(categories)
table(dataDf$yVarReturnCatLabels, exclude = NULL)


# Plot the categories
ggplot(dataDf, aes(x = yVarReturnCat)) +
  geom_bar() +
  ggtitle("Distribution of Brent Price Returns") +
  labs(y = "Number of Months", x = "Monthly Price Change") +
  theme(plot.title = element_text(lineheight = .8, face = "bold", hjust = 0.5))



# Plot the transformed categories vs. time
plot(x = dataDf$date,
     y = dataDf$yVarReturnCatLabels,
     type = 'l',
     ylim = c(0, 7),
     ylab = 'Brent Returns Categories',
     main = 'Brent Return - categorical')


# Fit a multinomial logit
modelBrent <- multinom(yVarReturnCatLabels ~ BrentPrice,
                       data = dataDf)

