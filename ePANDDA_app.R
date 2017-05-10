Start<-print(Sys.time())

######################################### Load Required Libraries ###########################################
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
    Cluster<-makeCluster(3)
    } else {
    Cluster<-makeCluster(as.numeric(CommandArgument[1]))
    }

#############################################################################################################
######################################### DATA DOWNLOAD, EPANDDA ############################################
#############################################################################################################
# No functions at this time.

############################################ Download Datasets from API  ####################################

print(paste("download PBDB refs",Sys.time()))

# Increase the timeout option to allow for larger data downloads
options(timeout=300)

# Download references from the Paleobiology Database through the API
GotURL<-RCurl::getURL("https://paleobiodb.org/data1.2/colls/refs.csv?all_records")
PBDBRefs<-read.csv(text=GotURL,header=TRUE)

# Pull out only the needed columns and rename them to match GDDRefs
PBDBRefs<-PBDBRefs[,c("reference_no","author1last","pubyr","reftitle","pubtitle")]
colnames(PBDBRefs)<-c("pbdb_no","pbdb_author","pbdb_year","pbdb_title","pbdb_pubtitle")

# Change data types of PBDBRefs to appropriate types
PBDBRefs[,"pbdb_no"]<-as.numeric(as.character(PBDBRefs[,"pbdb_no"]))
PBDBRefs[,"pbdb_author"]<-as.character(PBDBRefs[,"pbdb_author"])
PBDBRefs[,"pbdb_year"]<-as.numeric(as.character(PBDBRefs[,"pbdb_year"]))
PBDBRefs[,"pbdb_title"]<-as.character(PBDBRefs[,"pbdb_title"])
PBDBRefs[,"pbdb_pubtitle"]<-as.character(PBDBRefs[,"pbdb_pubtitle"])

# Remove PBDB Refs with no title
PBDBRefs<-subset(PBDBRefs,nchar(PBDBRefs[,"pbdb_title"])>2)

# Find the current directory
CurrentDirectory<-getwd()

print(paste("download DD refs",Sys.time()))

# Move othe input folder
setwd(paste(CurrentDirectory,"/input",sep=""))
# Load in the input.bibjson file
GDDRefs<-fromJSON("input.bibjson") # if testing: "~/Documents/DeepDive/ePANDDA/EPANDDA_app-master/input/input.bibjson"

# Extract authors, docid, year, title, journal, and publisher information from the BibJson List into vectors
gdd_id<-parSapply(Cluster,GDDRefs,function(x) x[["_gddid"]])
gdd_author<-parSapply(Cluster,GDDRefs,function(x) paste(unlist(x[["author"]]),collapse=" "))
gdd_year<-parSapply(Cluster,GDDRefs,function(x) x[["year"]])
gdd_title<-parSapply(Cluster,GDDRefs,function(x) x[["title"]])
gdd_pubtitle<-parSapply(Cluster,GDDRefs,function(x) x[["journal"]])
gdd_publisher<-parSapply(Cluster,GDDRefs,function(x) x[["publisher"]])

# Create identically formatted data.frames for geodeepdive and pbdb references (overwrite GDDRefs)
GDDRefs<-as.data.frame(cbind(gdd_id,gdd_author,gdd_year,gdd_title,gdd_pubtitle, gdd_publisher),stringsAsFactors=FALSE)
    
# Change data types of DDRefs to appropriate types
GDDRefs[,"gdd_id"]<-as.character(GDDRefs[,"gdd_id"])
GDDRefs[,"gdd_author"]<-as.character(GDDRefs[,"gdd_author"])
GDDRefs[,"gdd_year"]<-as.numeric(as.character(GDDRefs[,"gdd_year"]))
GDDRefs[,"gdd_title"]<-as.character(GDDRefs[,"gdd_title"])
GDDRefs[,"gdd_pubtitle"]<-as.character(GDDRefs[,"gdd_pubtitle"])
GDDRefs[,"gdd_publisher"]<-as.character(GDDRefs[,"gdd_publisher"])
                         
# Update DDRefs[,"gdd_pubtitle"] to usgs bulletin where appropriate
print(paste("usgs bulletin update",Sys.time()))   
USGS_Bulletin<-which(GDDRefs[,"gdd_pubtitle"]=="Bulletin"&GDDRefs[,"gdd_publisher"]=="USGS")
GDDRefs[USGS_Bulletin,"gdd_pubtitle"]<-"usgs bulletin"
# update and overwrite gdd_pubtitle
gdd_pubtitle<-GDDRefs[,"gdd_pubtitle"]

# Convert the title and pubtitle to all lower case, because stringsim, unlike grep, cannot distinguish between cases
PBDBRefs[,"pbdb_title"]<-tolower(PBDBRefs[,"pbdb_title"])
PBDBRefs[,"pbdb_pubtitle"]<-tolower(PBDBRefs[,"pbdb_pubtitle"])
GDDRefs[,"gdd_title"]<-tolower(GDDRefs[,"gdd_title"])
GDDRefs[,"gdd_pubtitle"]<-tolower(GDDRefs[,"gdd_pubtitle"])
                                                 
# Record stats 
# Record the initial number of PBDB documents
PBDBDocs<-dim(PBDBRefs)[1]
# Record the initial number of GeoDeepDive documents
GDDDocs<-dim(GDDRefs)[1]   
                         
############################### HARMONIZE JOURNAL TITLES BETWEEN PBDB AND GDD ################################
print(paste("harmonize publication titles between pbdb and gdd",Sys.time()))

# replace all versions of "United States Geological Survey" in PBDB with "usgs" 
PBDBRefs[,"pbdb_pubtitle"]<-gsub("u.s. geological survey","usgs",PBDBRefs[,"pbdb_pubtitle"])
PBDBRefs[,"pbdb_pubtitle"]<-gsub("u. s. geological survey","usgs", PBDBRefs[,"pbdb_pubtitle"])
PBDBRefs[,"pbdb_pubtitle"]<-gsub("u.s.g.s.","usgs", PBDBRefs[,"pbdb_pubtitle"])       
PBDBRefs[,"pbdb_pubtitle"]<-gsub("us geological survey","usgs", PBDBRefs[,"pbdb_pubtitle"])
PBDBRefs[,"pbdb_pubtitle"]<-gsub("united states geological survey","usgs", PBDBRefs[,"pbdb_pubtitle"])
    
# For Geobios:
geobios<-which(PBDBRefs[,"pbdb_pubtitle"]=="géobios"| 
PBDBRefs[,"pbdb_pubtitle"]=="geobios mémoire spécial")
# Replace titles to match GeoDeepDive
PBDBRefs[geobios,"pbdb_pubtitle"]<-"geobios"
    
# For Canadian Journal of Earth Sciences:
canadian_journal<-which(PBDBRefs[,"pbdb_pubtitle"]=="canadian journal of earth science"|
PBDBRefs[,"pbdb_pubtitle"]=="canadian journal earth science")
# Replace titles to match GeoDeepDive
PBDBRefs[canadian_journal,"pbdb_pubtitle"]<-"canadian journal of earth sciences"    
    
#############################################################################################################
########################################## MATCH TITLES, EPANDDA ############################################
#############################################################################################################
# Find the best title stringsim for each PBDB ref in GDD
matchTitle<-function(x,y) {
    Similarity<-stringdist::stringsim(x,y)
    MaxTitle<-max(Similarity)
    MaxIndex<-which.max(Similarity)
    return(c(MaxIndex,MaxTitle))
    }

############################################ Initial Title Match Script #####################################       
# Status update
print(paste("perform title matches",Sys.time()))   

# Export the functions to the cluster
clusterExport(cl=Cluster,varlist=c("matchTitle","stringsim"))                         
                         
# Find the best title stringsim for each PBDB ref in GDD
TitleSimilarity<-parSapply(Cluster,PBDBRefs[,"pbdb_title"],matchTitle,GDDRefs[,"gdd_title"])
# Reshape the Title Similarity Output
TitleSimilarity<-as.data.frame(t(unname(TitleSimilarity)))
                         
# Bind Title Similarity by pbdb_no
InitialMatches<-cbind(PBDBRefs[,"pbdb_no"],TitleSimilarity)
InitialMatches[,"V1"]<-GDDRefs[InitialMatches[,"V1"],"gdd_id"]
colnames(InitialMatches)<-c("pbdb_no","gdd_id","title_sim")

# Merge initial matches, pbdb refs, and gdd refs
InitialMatches<-merge(InitialMatches,GDDRefs,by="gdd_id",all.x=TRUE)
InitialMatches<-merge(InitialMatches,PBDBRefs,by="pbdb_no",all.x=TRUE)
                        
# Status update
print(paste("finish title matches",Sys.time()))

#############################################################################################################
########################################## MATCH FIELDS, EPANDDA ############################################
#############################################################################################################                        
# A function for matching additional bibliographic fields between the best and worst match
matchAdditional<-function(InitialMatches) {
    # Whether the publication year is identical
    Year<-InitialMatches["pbdb_year"]==InitialMatches["gdd_year"]
    # The similarity of the journal names
    Journal<-stringsim(InitialMatches["pbdb_pubtitle"],InitialMatches["gdd_pubtitle"])
    # Whether the first author's surname is present in the GDD bibliography
    Author<-grepl(InitialMatches["pbdb_author"],InitialMatches["gdd_author"],perl=TRUE,ignore.case=TRUE)
    # Return output     
    FinalOutput<-setNames(c(InitialMatches["pbdb_no"],InitialMatches["gdd_id"],InitialMatches["title_sim"],Author,Year,Journal),c("pbdb_no","gdd_id","title_sim","author_in","year_match","pubtitle_sim"))
    return(FinalOutput)
    }                   
######################################### Match Additional Fields Script ####################################  
print(paste("perform additional matches",Sys.time()))

# Reset the data types; columns are sometimes coerced to the incorrect data type for unknown reasons
InitialMatches[,"pbdb_no"]<-as.numeric(InitialMatches[,"pbdb_no"])
InitialMatches[,"gdd_id"]<-as.character(InitialMatches[,"gdd_id"])
InitialMatches[,"title_sim"]<-as.numeric(InitialMatches[,"title_sim"])
InitialMatches[,"gdd_author"]<-as.character(InitialMatches[,"gdd_author"])
InitialMatches[,"gdd_year"]<-as.numeric(InitialMatches[,"gdd_year"])
InitialMatches[,"gdd_title"]<-as.character(InitialMatches[,"gdd_title"])
InitialMatches[,"gdd_pubtitle"]<-as.character(InitialMatches[,"gdd_pubtitle"]) 
InitialMatches[,"gdd_publisher"]<-as.character(InitialMatches[,"gdd_publisher"]) # This is where the break was happening
InitialMatches[,"pbdb_author"]<-as.character(InitialMatches[,"pbdb_author"])
InitialMatches[,"pbdb_year"]<-as.numeric(InitialMatches[,"pbdb_year"])
InitialMatches[,"pbdb_title"]<-as.character(InitialMatches[,"pbdb_title"])
InitialMatches[,"pbdb_pubtitle"]<-as.character(InitialMatches[,"pbdb_pubtitle"])

# export matchAdditional to the cluster
clusterExport(cl=Cluster,varlist=c("matchAdditional"))    
                         
# Perform the additional matches
MatchReferences<-parApply(Cluster, InitialMatches, 1, matchAdditional)
                         
# Stop the Cluser
stopCluster(Cluster)
                         
# Reformat MatchReferences
MatchReferences<-as.data.frame(t(MatchReferences),stringsAsFactors=FALSE)
    
print(paste("organize stats",Sys.time()))    
# Record Stats
# Create a vector of the title_sim column of MatchReferences
TitleSim<-as.numeric(as.character(MatchReferences[,"title_sim"]))
# Create a table showing title_sim values rounded to the nearest hundredth    
TitleSimTable<-table(round_any(TitleSim,0.1,floor))
RoundedTitleSim<-paste(names(TitleSimTable), collapse=" ")
Refs<-paste(TitleSimTable, collapse=" ")

# Create stat descriptions
Descriptions<-c("Date","Initial number of PBDBRefs","Initial number of GDDRefs","Rounded title similarities","Number of references")
# Create date and time record for stats file
Date<-as.character(Sys.time())
# Bind stats
Stats<-rbind(Date,PBDBDocs,GDDDocs,RoundedTitleSim,Refs)
Stats<-as.data.frame(cbind(Stats,Descriptions),row.names=FALSE)
colnames(Stats)<-c("Stats","Descriptions") 
    
print(Stats)
    
# Status Update    
print(paste("finish matches",Sys.time()))
                           
# Print status
print(paste("Writing Outputs",Sys.time()))
    
# Write the Outputs
setwd(paste(CurrentDirectory,"/output",sep=""))
# Clear any old output files
unlink("*")

# Write the output
write.csv(GDDRefs, "DDRefs.csv")
write.csv(PBDBRefs, "PBDBRefs.csv")
write.csv(MatchReferences, "MatchReferences.csv")
write.csv(Stats,"Stats.csv",row.names=FALSE)

# Print the completion notice
print(paste("Complete",Sys.time()))
