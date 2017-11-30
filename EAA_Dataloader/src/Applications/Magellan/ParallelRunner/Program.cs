// Make sure to use a drive that has at least 1500 IOPS to support 8 thread - CFL 5/15/17
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace RunParallelPython
{
    public class Program
    {
        // RunParallelPython1 zipDir outputDir
        public static void Main(string[] args)
        {
            Console.WriteLine("Pullling files from S3");
            var sprogram = "./Applications/Magellan/Magellan.py";

//  sends a process to log the start of this process Start Logging        
            var command = string.Format("{0} \"{1}\"", sprogram, "SL");  
            var processdownload = Process.Start("python", command);
            processdownload.WaitForExit();

//  sends a request process to pull down the data files from s3 Pull Data        
            command = string.Format("{0} \"{1}\"", sprogram, "PD");  
            processdownload = Process.Start("python", command);
            processdownload.WaitForExit();

//	This call performs all the multi-threading processing of the zip files
            Console.WriteLine("Processing files in: {0}", args[0]);
            var zipFileToProcess = Directory.GetFiles(args[0], "*.zip", SearchOption.TopDirectoryOnly);
            var batchsize = 100;
            var numrecs = zipFileToProcess.Length;
            var numiterations = 0;
            var partialIteraration = "N";
            if ((numrecs % batchsize) == 0)
            {
                numiterations = numrecs / batchsize;
            }
            else
            {
                numiterations = numrecs / batchsize + 1;
                partialIteraration = "Y";
            }
            string parameterlist = String.Format(
                "number of total files {0} number of iterations is {1} based on batch size of {2}",
                numrecs,numiterations,batchsize
            );
            Console.WriteLine("number of total files {0}",numrecs);
            Console.WriteLine("number of iterations is {0} based on batch size of {1} ", numiterations, batchsize);
            Console.WriteLine("Do we have a partial {0} ", partialIteraration);
//  make sure we log the parameters Log Parameters
            command = string.Format("{0} \"{1}\" \"{2}\"", sprogram, "LP", parameterlist);  
            processdownload = Process.Start("python", command);
            processdownload.WaitForExit();

//  the actual processing starting point Set Up Environment
            command = string.Format("{0} \"{1}\"", sprogram, "SE");  
            processdownload = Process.Start("python", command);
            processdownload.WaitForExit();

            List<string> fnames = new List<string>();
            int batchCount = 0;
            foreach(string fl in zipFileToProcess)
            {
                if (fnames.Count >= batchsize)
                {
                    batchCount = batchCount + 1;
                    ProcessBatch(batchCount, fnames, sprogram);
                    fnames = new List<string>();
                }  
                fnames.Add(fl);
            }
            if (partialIteraration == "Y")
            {
                batchCount = batchCount + 1;
                ProcessBatch(batchCount, fnames, sprogram);
            }
//  This loads all data Load Data
            command = string.Format("{0} \"{1}\"", sprogram, "LD");  
            var processloaddata = Process.Start("python", command);
            processloaddata.WaitForExit();            

// let everyone know we are done EL End Logging           
            command = string.Format("{0} \"{1}\"", sprogram, "EL");  
            processdownload = Process.Start("python", command);
            processdownload.WaitForExit();

            Console.WriteLine("Done processing.");
        }
        public static void ProcessBatch(int batchCount, List<string> zipFiles, string sprogram)
        {
            Console.WriteLine("Batch Number: {0}", batchCount);
            var command = "";
            Parallel.ForEach(zipFiles,
                new ParallelOptions { MaxDegreeOfParallelism = 8 },
                currentFile =>
            {
// Process the JSON                
                Console.WriteLine("Processing file: {0} on thread {1}", 
                                    currentFile, Thread.CurrentThread.ManagedThreadId);
                command = string.Format("{0} \"{1}\" \"{2}\"",sprogram, "PJ", currentFile);

                var processMT = Process.Start("python", command);
                processMT.WaitForExit();
            });

//	This last call cleans up the local 
            Console.WriteLine("clean up files used for Batch Number: {0}", batchCount);
            command = string.Format("{0} \"{1}\"", sprogram,  "CL");  
            var processclean = Process.Start("python", command);
            processclean.WaitForExit();
        }
    }
}