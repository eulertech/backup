cat("\014")
system("python /home/rstudio/EAA_Dataloader/src/Main.py /home/rstudio/EAA_Dataloader/src/ProcessStepsConfig.json")

#move the files to S3 bucket. Don't use the default move command due to the risk of corrupting the file in the event of an interruption
copyToS3AndDelete <- function(folderpath, filename) {
	#copy the files
	cmd <- paste0("aws s3 cp ",
			folderpath,
			as.character((Sys.Date())),
			filename,
			" s3://ihs-temp/varun/")  
	print(cmd)  
	system(cmd)
	
	#delete the files
	cmd <- paste0("rm -f ",
			folderpath,
			as.character((Sys.Date())),
			filename)
	
	#print(cmd)	
	#system(cmd)
}


copyToS3AndDelete("/home/rstudio/EAA_Dataloader_Data/output/Hindsight/QC/", "_post_redshift_rowcounts_seriesid.csv")
copyToS3AndDelete("/home/rstudio/EAA_Dataloader_Data/output/Hindsight/QC/", "_post_sql_server_rowcounts_seriesid.csv")
copyToS3AndDelete("/home/rstudio/EAA_Dataloader_Data/output/Hindsight/QC/", "_pre_sql_server_rowcounts_seriesid.csv")
copyToS3AndDelete("/home/rstudio/EAA_Dataloader_Data/output/Hindsight/QC/", "_post_mismatches.csv")
copyToS3AndDelete("/home/rstudio/EAA_Dataloader_Data/log/", "_applog.log")
copyToS3AndDelete("/home/rstudio/EAA_Dataloader_Data/output/Hindsight/QC/", "_etl_report.csv")

system("aws s3 ls s3://ihs-temp/varun/")
system("aws ec2 stop-instances --region us-west-2 --instance-ids i-00b03857dc5b58e9b")

