# Input: dataframe with columns as variables and rows as variable values
# Output: network visualization graph with links and relationships between those variables
# Author: Lou Zhang

wants <- c("visNetwork", "mRMRe", "MTS")
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")

EaaNetworkGraph <- function(df, linkStrength = 1, relationship = "nonlinear") {
  
  if(relationship = "nonlinear") {
  
  aMIM <- mRMR.data(df2)
  
  MIM <- mim(aMIM)
  
  MIM <- as.data.frame(MIM)
  
  for(i in 1:ncol(MIM)) {
    MIM[[i]][is.infinite(MIM[[i]])] <-0
  }
  
  b <- as.double(1:nrow(MIM))
  
  for(i in 1:ncol(MIM)) {
    a <- as.double(MIM[[i]])
    b <- cbind(b,a)
    
  }
  
  b <- b[,2:ncol(b)]
  
  rownames(b) <- rownames(MIM)
  colnames(b) <- colnames(MIM)
  
  MIMpairs <-as.data.frame(which(b>0, arr.ind = TRUE))
  
  MIMdouble <- b
  
  MIMdouble[ MIMdouble < linkStrength/100 ] <- 0
  MIMdouble[ MIMdouble > linkStrength/100 ] <- 1
  
  # Make network graph
  
  nodes <- reftable[reftable$series_id %in% colnames(MIMdouble),]
  nodes$id <- nodes$series_id
  
  rows <- as.data.frame(rownames(MIMdouble))
  rows$index <- rownames(rows)
  rows$lookup <- rows$`rownames(MIMdouble)`
  
  b <- as.data.frame(which(MIMdouble == 1, arr.ind = TRUE))
  
  for(i in 1:ncol(b)) {
    
    b[,i] <- vlookup(ref = b[,i],table = rows[2:3],column = 2)
    
  }
  
  for (i in 1:nrow(b)) {
    b[i, ] = sort(b[i, ])
  }
  
  b <- b[!duplicated(b),]
  
  links <- b
  
  }
  
  if(relationship = "linear"){
    
  mat <- ccm(x = df2)
  
  }
  
  nodes$weights <- 1
  nodes$geo <- as.integer(nodes$geo)
  nodes <- rbind(nodes, NA)
  nodes[nrow(nodes),1] <- "target"
  
  links$weight <- 1
  colnames(links) <- c("from","to","weight")
  rownames(links) <- index(links)
  
  links$from <- as.character(links$from)
  links$to <- as.character(links$to)
  
  nodes$shape <- "dot"  
  nodes$shadow <- TRUE # Nodes will drop shadow
  nodes$title <- nodes$shortlabel # Text on click
  nodes$label <- nodes$Wefa_mnemonic # Node label
  nodes$size <- nodes$X # Node size
  nodes$borderWidth <- 2 # Node border width
  
  nodes$color.background <- colors(distinct = TRUE)[nodes$geo]
  nodes$color.border <- "black"
  nodes$color.highlight.background <- "orange"
  nodes$color.highlight.border <- "darkred"
  
  visNetwork(nodes, links)
  
}
