Start<-print(Sys.time())

# Install libraries if necessary and load them into the environment
if (require("RCurl",warn.conflicts=FALSE)==FALSE) {
    install.packages("RCurl",repos="http://cran.cnr.berkeley.edu/");
    library("RCurl");
    }
    
if (require("RJSONIO",warn.conflicts=FALSE)==FALSE) {
    install.packages("RJSONIO",repos="http://cran.cnr.berkeley.edu/");
    library("RJSONIO");
    }

if (require("stringdist",warn.conflicts=FALSE)==FALSE) {
    install.packages("stringdist",repos="http://cran.cnr.berkeley.edu/");
    library("stringdist");
    }

if (require("doParallel",warn.conflicts=FALSE)==FALSE) {
    install.packages("doParallel",repos="http://cran.cnr.berkeley.edu/");
    library("doParallel");
    }

if (require("plyr",warn.conflicts=FALSE)==FALSE) {
    install.packages("plyr",repos="http://cran.cnr.berkeley.edu/");
    library("plyr");
    }

# Start a cluster for multicore, 4 by default or higher if passed as command line argument
CommandArgument<-commandArgs(TRUE)
if (length(CommandArgument)==0) {
    Cluster<-makeCluster(4)
    } else {
    Cluster<-makeCluster(as.numeric(CommandArgument[1]))
    }

print(paste("download PBDB refs",Sys.time()))

# download all references from PBDB
RefsURL<-"https://paleobiodb.org/data1.2/colls/refs.csv?all_records"
GotURL<-getURL(RefsURL)
PBDBRefs<-read.csv(text=GotURL,header=TRUE)

# Find the current directory
CurrentDirectory<-getwd()

print(paste("download DD refs",Sys.time()))

# Move othe input folder
setwd(paste(CurrentDirectory,"/input",sep=""))
# Load in the input.bibjson file
DDRefs<-fromJSON("input.bibjson")

print(paste("make DDRefs columns",Sys.time()))

print(paste("make a column of DD reference number",Sys.time()))
gdd_id<-parSapply(Cluster,DDRefs,function(x) x[["_gddid"]])
print(paste("make a vector of DD authors",Sys.time()))
gdd_author<-parSapply(Cluster,DDRefs,function(x) paste(unlist(x[["author"]]),collapse=" "))
print(paste("make a vector of DD publication years",Sys.time()))
gdd_year<-parSapply(Cluster,DDRefs,function(x) x[["year"]])
print(paste("make a vector of DD ref titles",Sys.time()))
gdd_title<-parSapply(Cluster,DDRefs,function(x) x[["title"]])
print(paste("make a column of DD journal names",Sys.time())) 
gdd_pubtitle<-parSapply(Cluster,DDRefs,function(x) x[["journal"]])
  
# create identically formatted matrices for geodeepdive and pbdb references 
DDRefs<-cbind(gdd_id,gdd_author,gdd_year,gdd_title,gdd_pubtitle)
PBDBRefs<-cbind(PBDBRefs[c("reference_no","author1last","pubyr","reftitle","pubtitle")])
colnames(PBDBRefs)<-c("pbdb_no","pbdb_author","pbdb_year","pbdb_title","pbdb_pubtitle")

# convert matrices to dataframes
DDRefs<-as.data.frame(DDRefs)
PBDBRefs<-as.data.frame(PBDBRefs)

# Change data types of DDRefs to appropriate types
DDRefs[,"gdd_id"]<-as.character(DDRefs[,"gdd_id"])
DDRefs[,"gdd_author"]<-as.character(DDRefs[,"gdd_author"])
DDRefs[,"gdd_year"]<-as.numeric(as.character(DDRefs[,"gdd_year"]))
DDRefs[,"gdd_title"]<-as.character(DDRefs[,"gdd_title"])
DDRefs[,"gdd_pubtitle"]<-as.character(DDRefs[,"gdd_pubtitle"])
# Change data types of PBDBRefs to appropriate types
PBDBRefs[,"pbdb_no"]<-as.numeric(as.character(PBDBRefs[,"pbdb_no"]))
PBDBRefs[,"pbdb_author"]<-as.character(PBDBRefs[,"pbdb_author"])
PBDBRefs[,"pbdb_year"]<-as.numeric(as.character(PBDBRefs[,"pbdb_year"]))
PBDBRefs[,"pbdb_title"]<-as.character(PBDBRefs[,"pbdb_title"])
PBDBRefs[,"pbdb_pubtitle"]<-as.character(PBDBRefs[,"pbdb_pubtitle"])
 
# Convert the title and pubtitle to all caps, because stringsim cannot distinguish between cases
PBDBRefs[,"pbdb_title"]<-tolower(PBDBRefs[,"pbdb_title"])
PBDBRefs[,"pbdb_pubtitle"]<-tolower(PBDBRefs[,"pubtitle"])
DDRefs[,"gdd_title"]<-tolower(DDRefs[,"gdd_title"])
DDRefs[,"gdd_pubtitle"]<-towlower(DDRefs[,"gdd_title"])
    
# RECORD STATS 
# Record the initial number of PBDB documents
PBDBDocs<-dim(PBDBRefs)[1]
# Record the initial number of GeoDeepDive documents
DDDocs<-dim(DDRefs)[1]

### Phase 2: A MATCHING FUNCTION IS BORN
matchTitle<-function(x,y) {
    Similarity<-stringsim(x,y)
    max_title<-max(Similarity)
    max_no<-which.max(Similarity)
    return(c(max_no,max_title))
    }
    
# Status update
print(paste("perform title matches",Sys.time()))   
    
# Find the best title stringsim for each PBDB ref in GDD
clusterExport(cl=Cluster,varlist=c("matchTitle","stringsim"))
TitleSimilarity<-parSapply(Cluster,PBDBRefs[,"pbdb_title"],matchTitle,DDRefs[,"gdd_title"])
# Reshape the Title Similarity Output
TitleSimilarity<-as.data.frame(t(unname(TitleSimilarity)))
    
# Bind Title Similarity with pbdb_no
InitialMatches<-cbind(PBDBRefs[,"pbdb_no"],TitleSimilarity)
InitialMatches[,"V1"]<-DDRefs[InitialMatches[,"V1"],"gdd_id"]
colnames(InitialMatches)<-c("pbdb_no","gdd_id","title_sim")

# Merge initial matches, pbdb refs, and gdd refs
InitialMatches<-merge(InitialMatches,DDRefs,by="gdd_id",all.x=TRUE)
InitialMatches<-merge(InitialMatches,PBDBRefs,by="pbdb_no",all.x=TRUE)
    
# Status update
print(paste("finish title matches",Sys.time()))

### Phase 3: Matching additional similarity information
print(paste("perform additional matches",Sys.time()))
    
# A function for matching PBDB and DDRefs
matchAdditional<-function(InitialMatches) {
    # Pub year match
    Year<-InitialMatches["pbdb_year"]==InitialMatches["gdd_year"]
    # Journal Similarity
    Journal<-stringsim(InitialMatches["pbdb_pubtitle"],InitialMatches["gdd_pubtitle"])
    # Author present
    Author<-grepl(InitialMatches["pbdb_author"],InitialMatches["gdd_author"],perl=TRUE,ignore.case=TRUE)
    # Return output     
    FinalOutput<-setNames(c(InitialMatches["pbdb_no"],InitialMatches["gdd_id"],InitialMatches["title_sim"],Author,Year,Journal),c("pbdb_no","gdd_id","title_sim","author_in","year_match","pubtitle_sim"))
    return(FinalOutput)
    }

# export matchAdditional to the cluster
clusterExport(cl=Cluster,varlist=c("matchAdditional"))                           
MatchReferences<-parApply(Cluster, InitialMatches, 1, matchAdditional)
# Stop the Cluser
stopCluster(Cluster)
# Reformat MatchReferences
MatchReferences<-as.data.frame(t(MatchReferences))
  
    
print(paste("organize stats",Sys.time()))    
# RECORD STATS
# Create a vector of the title_sim column of MatchReferences
TitleSim<-as.numeric(as.character(MatchReferences[,"title_sim"]))
# Create a table showing title_sim values rounded to the nearest hundredth    
TitleSimTable<-table(round_any(TitleSim,0.1,floor))
RoundedTitleSim<-paste(names(TitleSimTable), collapse=" ")
Refs<-paste(TitleSimTable, collapse=" ")

# Create stat descriptions
Descriptions<-c("Date","Initial number of PBDBRefs","Initial number of DDRefs","Rounded title similarities","Number of references")
# Create date and time record for stats file
Date<-as.character(Sys.time())
# Bind stats
Stats<-rbind(Date,PBDBDocs,DDDocs,RoundedTitleSim,Refs)
Stats<-as.data.frame(cbind(Stats,Descriptions),row.names=FALSE)
colnames(Stats)<-c("Stats","Descriptions")  
    
# Status Update    
print(paste("finish matches",Sys.time()))
                           
# Print status
print(paste("Writing Outputs",Sys.time()))
    
# Write the Outputs
setwd(paste(CurrentDirectory,"/output",sep=""))
# Clear any old output files
unlink("*")

# Write the output
saveRDS(MatchReferences, "MatchReferences.rds")
saveRDS(DDRefs, "DDRefs.rds")
saveRDS(PBDBRefs, "PBDBRefs.rds")
write.csv(MatchReferences, "MatchReferences.csv")
write.csv(Stats,"Stats.csv",row.names=FALSE)

# Print the completion notice
print(paste("Complete",Sys.time()))
