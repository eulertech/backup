

# Increase size of memory for Java
options(java.parameters="-Xmx8192m")


# Snippet of code to increase memory limits
AADiag_MemAllocTest(blockSize = 5000L,
                    blockCount = 1L,
                    releaseMem = TRUE,
                    verbose = TRUE)

gc()