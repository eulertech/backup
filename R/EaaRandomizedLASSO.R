# Stability selection in the spirit of Meinshausen&Buhlman
# Input: df: dataframe with target at first column and rest after
#				 nboostrap : number of bootstrap
# Output: dataframe with two columns: varnames, coeficient
# Liang Kuang, 04/19/2017

EaaRandomLASSO <- function(df,nbootstrap=200,alpha=0.2,allfeatures=TRUE, sorted=TRUE)
{

  wants <- c("glmnet")
  has   <- wants %in% rownames(installed.packages())
  if(any(!has)) install.packages(wants[!has])
  sapply(wants, require, character.only = TRUE)
  rm("wants","has")

	range01 <- function(x){(x-min(x))/(max(x)-min(x))}

	y <- apply(as.matrix(df[1]),2,as.numeric)
    x <- apply(as.matrix(df[2:ncol(df)]), 2, as.numeric)
	dimx <- dim(x)
	n <- dimx[1]
	p <- dimx[2]
	halfsize <- as.integer(n/2)
	freq <- matrix(0,nrow=p, ncol=1)

	for (i in seq(nbootstrap)) {
		# Randomly reweight each variable
		xs <- t(t(x)*runif(p,alpha,1))
		# Ramdomly split the sample in two sets
		perm <- sample(dimx[1])
		i1 <- perm[1:halfsize]
		i2 <- perm[(halfsize+1):n]

		# run the randomized lasso on each sample and check which variables are selected
		x.train <- xs[i1,]
		y.train <- y[i1]
		cvfit <- cv.glmnet(x.train,y.train)
		lm.fit <- cvfit$glmnet.fit
		coeflambdamin <- coef(cvfit, lambda = 'lambda.min')[2:(p+1)]
		freq <- freq + abs(sign(coeflambdamin))

		x.train <- xs[i2,]
		y.train <- y[i2]
		cvfit <- cv.glmnet(x.train,y.train)
		coeflambdamin <- coef(cvfit, lambda = 'lambda.min')[2:(p+1)]
		freq <- freq + abs(sign(coeflambdamin))
		}

	# normalize frequence in [0,1]
	# the final stability score is the maximum frequency over the steps
	freq <- freq/(2*nbootstrap)


	results <- data.frame(coef.name = colnames(df)[2:(p+1)], frequency = freq)
	if(allfeatures==FALSE){
		results <- results[results$frequency !=0,]
	}
	if(sorted == TRUE){
		results <- results[order(-results[,2]), ]
	}

	return(freq)
}
