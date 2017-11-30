###############################################################################
## SnowClusterTools.R
##
##   This source file is part of Advanced Analytics AWS Cloud Tools
##
##   Author:  Christopher Lewis and Marc Veillet
##
## License: (c) IHSMarkit 2016-17 - not to be used/shared outside the company
###############################################################################

# Private function used by QueryEC2Instances(), StartEC2Instances(), StopEC2Instances() etc. to obtain the list of
# IDs of the EC2 hosts from the set of arguments to these functions.
# This function also provides much type and value checking for these arguments.
GetEc2InstanceIdsFromFctArgs <- function(configAWS = NULL, ec2Ids = NULL, ec2IdsFile = NULL, ec2InstancesSubset = NULL, callingFct = NULL) {
  if (is.null(callingFct))
    callingFct <- "GetEc2InstanceIdsFromFctArgs"

  retVal <- NULL

  if (!is.null(ec2Ids)) {
    if (!is.character(ec2Ids) && !is.factor(ec2Ids)) {
      stop(sprintf("Invalid argument 'ec2Ids' to function %s; expecting one or seveal strings (character vector)", callingFct))
    }
    retVal <- as.character(ec2Ids)
  }

  if (is.null(retVal) && !is.null(ec2IdsFile)) {
    if (!is.character(ec2IdsFile)) {
      stop(sprintf("Invalid argument 'ec2IdsFile' to function %s; expecting a character string.", callingFct))
    }
    if (!file.exists(ec2IdsFile)) {
      stop(sprintf("'ec2IdsFile' argument to %s refers to an inexitant file.", callingFct))
    }
    id_ip <- read.table(file = ec2IdsFile, sep="," , header = TRUE, stringsAsFactors = FALSE)
    if (is.null(id_ip) || nrow(id_ip) == 0 || is.null(id_ip$id)) {
      stop(sprintf("%s :Could not get a proper list of EC2 IDs from File %s.", callingFct, ec2IdsFile))
    }
    retVal <- id_ip$id
  }

  if (is.null(retVal) && !is.null(configAWS)) {
    if (!is.list(configAWS)) {
      stop(sprintf("Invalid argument 'configAWS' to function %s; expecting an object like these returned by ConfigureAWS() function.",
                   callingFct))
    }
    if (is.null(configAWS$configuration$cluster$id_ip)) {
      stop(sprintf("The config passed to %s in the 'configAWS' argument does not include a proper defintion of the processing cluster", callingFct))
    }
    # these are often loaded as factors, hence the as.character
    retVal <- as.character(configAWS$configuration$cluster$id_ip$id)
  }

  if (is.null(retVal))
    return(NULL)

  # *** Subset the EC2 instances list if so requested ***
  if (!is.null(ec2InstancesSubset)) {
    if (!all(as.integer(ec2InstancesSubset) == ec2InstancesSubset)) {
      stop(sprintf("Invalid argument 'ec2InstancesSubset' to %s funciton; expecting an integer vector", callingFct))
    }
    if (length(retVal) < length(ec2InstancesSubset)) {
      stop(sprintf("Invalid argument 'ec2InstancesSubset' to %s funciton; this vector is longer than the number of EC2 instances.", callingFct))
    }
    retVal <- retVal[ec2InstancesSubset]
  }

  retVal
}

#' Report the status of individual EC2 servers.
#'
#' \code{QueryEC2Instances} Assert the 'stopped' vs. 'running' status of the EC2 hosts.
#'
#' This function can be used in an ad-hoc fashion or programmatically to determine the status
#' of each of the EC2 hosts of the [typically on-demand] processing cluster.  Use
#' \code{AreEC2InstancesRunning()} instead if your purpose is to check that the cluster, as a
#' whole is running.
#'
#' Most projects are associated with a processing cluster, defined in a \code{".dat"} file referenced
#' in the config file.  Such clusters are typically started and stopped as the need arises,  using
#' \code{StartEC2Instances} and \code{StopEC2Instances}.  SNOW clusters can be built from the CPUs
#' on these EC2 hosts.
#'
#' \strong{Pre-requisite}:  The AWS client (AWS CLI) must be installed on the "edge node", i.e. on
#' the worksation or server where this function is called from.
#'
#' @param configAWS object returned by \code{ConfigureAWS}.  This holds the all configuration's
#'   parameters, in particular the IP addresses and other details associated with the processing
#'   cluster.  Pass this argument and leave \code{ec2Ids}, \code{ec2IdsFile} as \code{NULL}
#'   if you with to start/stop the project's processing cluster.
#' @param ec2Ids character vector : the AWS IDs of the EC2 instance one wish to start or stop.
#' @param ec2IdsFile characater string : a file name, with optional path.  This file is a
#'  comma-delimited file similar to the \code{.dat} files where the processing clusters are
#'  defined. The file must include a header and the column where the EC2 IDs is to be found
#'  must be named \code{id} (lowercase).
#' @param ec2InstancesSubset integer vector : an optional argument used to specify
#'   the index within the list of servers defined by the \code{configAWS},  \code{ec2Ids}
#'   or \code{ec2IdsFile} which should effectively be started or stopped.  It is used
#'   for example when the processing cluster is made of very many servers, but for a
#'   small parallel job, we only wish to use a few.
#' @return A character vector with one element per host in the cluster, indicating the run/stop
#'   status of each of these hosts.
#'
#' @examples
#' myConfig <- AACloudTools::ConfigureAWS("Config/config.json")
#' AACloudTools::QueryEC2Instances(myConfig)
#' # [1] "stopped" "stopped"
#'
#' @seealso \code{\link{AreEC2InstancesRunning}}
#'
#' @family SNOW and Parallel Processing helper functions
#' @export
QueryEC2Instances <- function(configAWS = NULL, ec2Ids = NULL, ec2IdsFile = NULL, ec2InstancesSubset = NULL) {
  ec2Ids <- GetEc2InstanceIdsFromFctArgs(configAWS = configAWS, ec2Ids = ec2Ids, ec2IdsFile = ec2IdsFile, callingFct = "QueryEC2Instances")

  commandLine <- paste0("aws ec2 describe-instances --region us-west-2 --instance-ids ", paste(ec2Ids, collapse = " "))
  retValue <- system(commandLine, intern = TRUE)
  data <- jsonlite::fromJSON(retValue)
  instances <- data$Reservations$Instances

  instances[[1]]$State$Name
}

#' Assert that all hosts of processing cluster are running.
#'
#' \code{AreEC2InstancesRunning} Returns \code{TRUE} if all nodes in processing cluster are
#'    running.
#'
#' This function can be used in an ad-hoc fashion or programmatically to determine if the
#' cluster can be used.
#'
#' Most projects are associated with a processing cluster, defined in a \code{".dat"} file referenced
#' in the config file.  Such clusters are typically started and stopped as the need arises,  using
#' \code{StartEC2Instances} and \code{StopEC2Instances}.  SNOW clusters can be built from the CPUs
#' on these EC2 hosts.
#'
#' \strong{Pre-requisite}:  The AWS client (AWS CLI) must be installed on the "edge node", i.e. on
#' the worksation or server where this function is called from.
#'
#' @param configAWS object returned by \code{ConfigureAWS}.  This holds the all configuration's
#'   parameters, in particular the IP addresses and other details associated with the processing
#'   cluster.  Pass this argument and leave \code{ec2Ids}, \code{ec2IdsFile} as \code{NULL}
#'   if you with to start/stop the project's processing cluster.
#' @param ec2Ids character vector : the AWS IDs of the EC2 instance one wish to start or stop.
#' @param ec2IdsFile characater string : a file name, with optional path.  This file is a
#'  comma-delimited file similar to the \code{.dat} files where the processing clusters are
#'  defined. The file must include a header and the column where the EC2 IDs is to be found
#'  must be named \code{id} (lowercase).
#' @param ec2InstancesSubset integer vector : an optional argument used to specify
#'   the index within the list of servers defined by the \code{configAWS},  \code{ec2Ids}
#'   or \code{ec2IdsFile} which should effectively be started or stopped.  It is used
#'   for example when the processing cluster is made of very many servers, but for a
#'   small parallel job, we only wish to use a few.
#' @return \code{TRUE} if all hosts in cluster are running, \code{FALSE} otherwise.
#'
#' @examples
#' myConfig <- AACloudTools::ConfigureAWS("Config/config.json")
#' AACloudTools::AreEC2InstancesRunning(myConfig)
#' # [1] FALSE
#'
#' @family SNOW and Parallel Processing helper functions
#' @export
AreEC2InstancesRunning <- function(configAWS = NULL, ec2Ids = NULL, ec2IdsFile = NULL, ec2InstancesSubset = NULL) {
  ec2Ids <- GetEc2InstanceIdsFromFctArgs(configAWS = configAWS, ec2Ids = ec2Ids, ec2IdsFile = ec2IdsFile, callingFct = "AreEC2InstancesRunning")
  status <- QueryEC2Instances(ec2Ids = ec2Ids)

  # The cluster, as a whole, is deemed 'running' when all its instances have a status of 'running'
  all(status == "running")
}

#' Start or stop EC2 Instances.
#'
#' Functions used to start or stop Virtual Computers in Amazon Cloud.
#'
#' \code{StartEC2Instances} Start EC2 instances.
#'
#' \code{StopEC2Instances} Stop EC2 instances.
#'
#' The instances to be started or stopped can be specified by one of three
#' arguments: \code{configAWS}, \code{ec2Ids}, \code{ec2IdsFile}
#'
#' If the server(s) referenced by these arguments is (are) readily running, invoking
#' \code{StartEC2Instances()} has no effect; converserly, invoking \code{StopEC2Instances()}
#' has no effect on readily stopped server(s).
#'
#' Most projects are associated with a processing cluster, defined in a \code{".dat"} file referenced
#' in the config file.  Such clusters are typically started and stopped as the need arises.
#' The \code{configAWS} can be used to refer to the project's cluster.
#' SNOW or Spark clusters can then be built from the CPUs on these EC2 hosts.
#'
#' For starting instances other than the project's designated processing cluster,
#' the \code{ec2Ids} or \code{ec2IdsFile} can be used; this is convenient for example
#' to start an on-demand RStudio server.
#'
#' Typically one will wait a few seconds and invoke \code{AreEC2InstancesRunning()} to assert that
#' the all servers referenced in the \code{StartEC2Instances()} call are effectively running.
#' The \code{wait} argument allows including this check as part of the \code{StartEC2Instances()} call.
#'
#' \strong{Pre-requisite}:  The AWS client (AWS CLI) must be installed on the worksation
#'   or server where this function is called from.  (The AWS CLI is not required on the servers
#'   started or stopped by these functions, unless of course they too would require the CLI
#'   for accessing various AWS services)
#'
#' \strong{Note:} These functions only work if the \code{ConfigureAWS()} function has
#'   previously been call, typically during the application initialization's phase.
#'
#' @param configAWS object returned by \code{ConfigureAWS}.  This holds the all configuration's
#'   parameters, in particular the IP addresses and other details associated with the processing
#'   cluster.  Pass this argument and leave \code{ec2Ids}, \code{ec2IdsFile} as \code{NULL}
#'   if you with to start/stop the project's processing cluster.
#' @param ec2Ids character vector : the AWS IDs of the EC2 instance one wish to start or stop.
#' @param ec2IdsFile characater string : a file name, with optional path.  This file is a
#'  comma-delimited file similar to the \code{.dat} files where the processing clusters are
#'  defined. The file must include a header and the column where the EC2 IDs is to be found
#'  must be named \code{id} (lowercase).
#' @param ec2InstancesSubset integer vector : an optional argument used to specify
#'   the index within the list of servers defined by the \code{configAWS},  \code{ec2Ids}
#'   or \code{ec2IdsFile} which should effectively be started or stopped.  It is used
#'   for example when the processing cluster is made of very many servers, but for a
#'   small parallel job, we only wish to use a few.
#' @param wait integer : the maximun amount of time, expessed in seconds, the function
#'   should wait before returning if the EC2 instances are not started. Use \code{0} to
#'   force the function to return at once without checking if the instances are started.
#' @param verbose logical : when \code{TRUE}, informational messages are printed to the
#'   console.
#'
#' @return \code{StartEc2Instances()} returns \code{TRUE} if all instances are started,
#'   \code{FALSE} otherwise.  \code{StopEc2Instances()} returns \code{TRUE} if all instances
#'   are stopped, \code{FALSE} otherwise.
#'  Also, when \code{verbose} argument is set to \code{TRUE} both function output a small
#'  JSON object to the console, showing the names of all the hosts in the cluster and
#'  their current and previous status.
#'
#' @examples
#' myConfig <- AACloudTools::ConfigureAWS("Config/config.json")
#' AACloudTools::StopEC2Instances(configAWS = myConfig)
#' # {
#' # "StoppingInstances": [
#' #   {
#' #     "InstanceId": "i-0cfc18f6ea6bc59e9",
#' #     "CurrentState": {
#' #       "Code": 80,
#' #       "Name": "stopped"
#' #     },
#' #     "PreviousState": {
#' #       "Code": 80,
#' #       "Name": "stopped"
#' #     }
#' #   },
#' #   {
#' #     "InstanceId": "i-03c3f49387262edb1",
#' #     "CurrentState": {
#' #       "Code": 80,
#' #       "Name": "stopped"
#' #     },
#' #     "PreviousState": {
#' #       "Code": 80,
#' #       "Name": "stopped"
#' #     }
#' #   }
#' #   ]
#' # }
#' @family SNOW and Parallel Processing helper functions
#' @name Start_Stop_EC2
#' @export
StartEC2Instances <- function(configAWS = NULL, ec2Ids = NULL, ec2IdsFile = NULL,
                              ec2InstancesSubset = NULL,
                              wait = 5, verbose = TRUE) {
  ec2Ids <- GetEc2InstanceIdsFromFctArgs(configAWS = configAWS, ec2Ids = ec2Ids, ec2IdsFile = ec2IdsFile, callingFct = "StartEC2Instances")
  commandLine <- paste0("aws ec2 start-instances --region us-west-2 --instance-ids ", paste(ec2Ids, collapse = " "))
  system(commandLine, show.output.on.console = verbose)
  if (wait > 0) {
    while (wait > 0) {
      wait <- wait - 3
      Sys.sleep(3)
      if (AreEC2InstancesRunning(ec2Ids = ec2Ids)) {
        if (verbose) {
          if (length(ec2Ids) > 1) {
            message(sprintf("All %d instances are running!", length(ec2Ids)))
          } else {
            message(sprintf("Instance %s is running!", ec2Ids))
          }
        }
        return(TRUE)
      }
    }
    if (verbose) {
      message("ATTENTION! Some or all instances are not yet running!")
      message("Please wait a few seconds and use AreEC2InstancesRunning() to assert if they are running.")
    }
    return(FALSE)
  } else {
    if (verbose) {
      message("Please wait a few seconds and use AreEC2InstancesRunning() to assert if instances are running.")
    }
    return(FALSE)
  }
}

#' @rdname Start_Stop_EC2
#' @export
StopEC2Instances <- function(configAWS = NULL, ec2Ids = NULL, ec2IdsFile = NULL, ec2InstancesSubset = NULL, verbose = TRUE) {
  ec2Ids <- GetEc2InstanceIdsFromFctArgs(configAWS = configAWS, ec2Ids = ec2Ids, ec2IdsFile = ec2IdsFile, callingFct = "StopEC2Instances")
  commandLine <- paste0("aws ec2 stop-instances  --region us-west-2 --instance-ids ", paste(ec2Ids, collapse = " "))
  system(commandLine, show.output.on.console = verbose)
  return(FALSE)
}


# private function to create a SNOW cluster on a processing cluster
MakeAASnowCluster <- function(configAWS, coresPerNode) {

  # It is unsure at this time that it would be useful or necessary to add this to the PATH
  # For sure when the underlying user is not allowed to SSH to the nodes of the cluster, the errors reported
  # when this is is the path and when it is not look different...
  curPath <- Sys.getenv("PATH")
  pathToPostBack <- ":/usr/lib/rstudio-server/bin/postback"
  if (!grepl(pathToPostBack, curPath, fixed=TRUE)) {
    Sys.setenv(PATH = paste0(curPath , pathToPostBack))
  }


  id_ip <- configAWS$configuration$cluster$id_ip
  ipList <- lapply(id_ip$ip, function(ip) { rep(ip, coresPerNode) })
  ipList <- unlist(ipList)
  cl <- makeCluster(ipList, type="SOCK", user="snow")
  cl
}



#' Start or stop a SNOW cluster.
#'
#' Functions used to start or stop a SNOW (Simple Network Of 'Workstations') cluster.
#'
#' \code{AAStartSnow} Start SNOW cluster.
#'
#' \code{AAStopSnow} Stop SNOW Cluster
#'
#' Most projects are associated with a processing cluster, defined in a \code{".dat"} file referenced
#' in the config file.  Such clusters are typically started and stopped as the need arises,  using
#' these functions.  SNOW clusters are be built from the CPUs on these EC2 hosts, although it is
#' also possible to build a SNOW cluster from the CPUs found on the local host.
#'
#'  By design, it is not possible to start an "external" SNOW cluster (i.e. one based on hosts beyond
#'  the local host), from  a [Windows] workstation.  Such external clusters should be started/used from a
#'  cloud-hosted RStudio server.
#'  The intent behind this limitation is to avoid the potential connection bottleneck between the
#'  workstation and the cloud.  It is however possible to use SNOW from a workstation,
#'  use the \code{local=TRUE} argument for that purpose.  (It may be necessary to limit the amount of
#'  work submitted to a local SNOW cluster, relative to that we submit to a typically much more
#'  powerful cloud-based Processing cluster).
#'
#'  The \code{coresPerNode} argument defines how many CPUs are used on each host of the processing
#'  cluster; this argument has no effect when \code{local=TRUE} as in this case all of the
#'  CPUs of the local host are used.
#'
#' \strong{Pre-requisite}:  The AWS client (AWS CLI) must be installed on the "edge node", i.e. on
#' the worksation or server where this function is called from.
#'
#' @param local logical : When \code{TRUE}, the CPUs of the local host are used to create the SNOW
#'   cluster.  When \code{FALSE}, the CPUs available on the hosts of the processing cluster referenced
#'   in the config (\code{configAWS}) are used.
#' @param configAWS object returned by \code{ConfigureAWS}.  This holds the all configuration's
#'   parameters, in particular the IP addresses and other details associated with the processing
#'   cluster.  This parameter is not necessary when \code{local=TRUE}.
#' @param coresPerNode integer : the number of CPUs we want to use in each of the hosts of the
#'   processing cluster.
#' @param cl cluster object : the cluster object, as returned by the \code{AAStartSnow} function.
#' @param localOnServerOverride integer : The number of cores to use on a shared RStudio Server, for
#'   a local SNOW cluster.  Leave this parameter to its NULL default, unless you have special
#'   authorization.
#'
#' @return \code{AAStartSnow} returns a SNOW cluster object, which purpose is mainly that of
#'   being passed to the \code{AAStopSnow}.
#'
#' @examples
#' \dontrun{
#' myConfig <- AACloudTools::ConfigureAWS("Config/config.json")
#' AACloudTools::StartEC2Instances(myConfig)
#' Sys.sleep(10)   # wait for the instances to be started
#' if (!AreEC2InstancesRunning(myConfig))
#'   stop("The Processing cluster did not start in due time.")
#' # only using 8 CPUs per node, because, say, the cluster will used by another process... ?)
#' mySnow <- AAStartSnow(local=FALSE, myConfig, coresPerNode=8)
#'
#' # here for some call to functions making use of the SNOW cluster, whether SNOW-aware
#' # functions or explicit constructs using  foreach() %dopar%
#'
#' # When done, stop the SNOW cluster
#' AAStopSnow(mySnow, myConfig)
#'
#' # and (if not used elsewhere...) stop the processing cluster itself as well
#' StopEC2Instances(myConfig)
#' }
#'
#' @family SNOW and Parallel Processing helper functions
#' @name Start_Stop_SNOW
#' @export
#' @export
AAStartSnow <- function(local=TRUE, configAWS=NULL, coresPerNode=16, localOnServerOverride=NULL) {
  cl <- NULL

  maxCores <- parallel::detectCores()

  if (local) {
    if (IsHostRStudioServer()) {
      # *** Throttle demands for local SNOW cluster on shared servers ***
      if (is.null(localOnServerOverride) || !is.integer(localOnServerOverride))
        localOnServerOverride <- 2
      if (maxCores > 2)
        maxCores <- maxCores - 1  # Spare one core,regardless of possible override request.
      nbCoresToUse <- min(localOnServerOverride, maxCores)

      warning(paste("AAStartSnow(): Local SNOW cluster on Shared RStudio Server should not be used to",
                    "launch a local SNOW cluster;  consider using a true cluster",
                    sprintf("A local cluster was started, but with only %d CPUs", nbCoresToUse))
             )
      cl <- makeCluster(nbCoresToUse)
    } else {
      cl <- makeCluster(min(coresPerNode, maxCores))
    }
  }
  else {
    if (.Platform$OS.type != "unix") {
      message(Sys.time(), " - Snow cluster can only be accessed by running this code on the IHSMarkit cloud RStudio instance.")
      if (.Platform$OS.type == "windows")
        message(Sys.time(), " - To run this code on windows, use AAStartSnow(local=TRUE) to run on your local machine with a smaller data set.")
      stop("Cannot continue in cluster mode!")
    }

    if (!AreEC2InstancesRunning(configAWS))
      stop("Cluster EC2 instances are not running!\n  Start the cluster EC2 instances using: StartEC2Instances(configAWS)")

    message("Cluster EC2 instances are running.  Starting snow...")
    cl <- MakeAASnowCluster(configAWS, coresPerNode)
  }

  registerDoSNOW(cl)

  cl
}

#' @rdname Start_Stop_SNOW
#' @export
AAStopSnow <- function(cl, configAWS) {
  stopCluster(cl)

  if (AreEC2InstancesRunning(configAWS))
    message("\nNOTE: Cluster EC2 instances are still running and will continue to cost us by the hour!\nStop the cluster EC2 instances if no longer needed using: StopEC2Instances(configAWS)")
}
