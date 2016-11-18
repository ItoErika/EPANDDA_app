library("RCurl")
library("RJSONIO")
library("stringdist")
library("doParallel")

# download all references from PBDB
RefsURL<-"https://paleobiodb.org/data1.2/taxa/refs.csv?select=taxonomy&private&all_records"
GotURL<-getURL(RefsURL)
PBDBRefs<-read.csv(text=GotURL,header=TRUE)

# download all article data form geodeepdive with a pubname that contains the word "palaeogeography"
DDRefs<-fromJSON("https://geodeepdive.org/api/articles?pubname_like=palaeogeography")
DDRefs<-DDRefs[[1]][[2]]

# make a column of DD reference numbers
DDRefNums<-sapply(DDRefs,function(x) x[["id"]])

# make a vector of DD authors
DDAuthors<-sapply(DDRefs,function(x) paste(unlist(x[["author"]]),collapse=" "))
 
# make a vector of DD publication years
DDPubYr<-sapply(DDRefs,function(x) x[["year"]])

# make a vector of DD ref titles 
DDTitles<-sapply(DDRefs,function(x) x[["title"]])
  
# make a column of DD jornalnames 
DDJournals<-sapply(DDRefs,function(x) x[["journal"]])

# create identically formatted matrices for geodeepdive and pbdb references 
DDRefs<-cbind(DDRefNums,DDAuthors,DDPubYr,DDTitles,DDJournals)
PBDBRefs<-cbind(PBDBRefs[c("reference_no","author1last","pubyr","reftitle","pubtitle")])

# convert matrices to dataframes
DDRefs<-as.data.frame(DDRefs)
PBDBRefs<-as.data.frame(PBDBRefs)

# make sure all of the data in the data frames are formatted correctly
DDRefs[,"DDRefNums"]<-as.character(DDRefs[,"DDRefNums"])
DDRefs[,"DDAuthors"]<-as.character(DDRefs[,"DDAuthors"])
DDRefs[,"DDPubYr"]<-as.numeric(as.character(DDRefs[,"DDPubYr"]))
DDRefs[,"DDTitles"]<-as.character(DDRefs[,"DDTitles"])
DDRefs[,"DDJournals"]<-as.character(DDRefs[,"DDJournals"])

DDRefs[,"author"]<-DDRefs[,"DDAuthors"]

colnames(DDRefs)[1]<-"reference_no"
colnames(DDRefs)[2]<-"author"
colnames(DDRefs)[3]<-"pubyr"
colnames(DDRefs)[4]<-"title"
colnames(DDRefs)[5]<-"pubtitle"
 
PBDBRefs[,"reference_no"]<-as.numeric(as.character(PBDBRefs[,"reference_no"]))
PBDBRefs[,"author1last"]<-as.character(PBDBRefs[,"author1last"])
PBDBRefs[,"pubyr"]<-as.numeric(as.character(PBDBRefs[,"pubyr"]))
PBDBRefs[,"reftitle"]<-as.character(PBDBRefs[,"reftitle"])
PBDBRefs[,"pubtitle"]<-as.character(PBDBRefs[,"pubtitle"])

colnames(PBDBRefs)[1]<-"reference_no"
colnames(PBDBRefs)[2]<-"author"
colnames(PBDBRefs)[3]<-"pubyr"
colnames(PBDBRefs)[4]<-"title"
colnames(PBDBRefs)[5]<-"pubtitle"

# subset PBDBRefs to only 100 references
PBDBRefs<-PBDBRefs[1:100,]

### Phase 2: A MATCHING FUNCTION IS BORN
    
print(paste("perform title matches",Sys.time()))   
    
# A function for matching PBDB and DDRefs
matchTitles<-function(Bib1,Bib2) {
    # Title Similarity
    Title<-stringsim(Bib1["title"],Bib2["title"])
    DocID<-as.character(Bib1["reference_no"])
    # Return output     
    return(setNames(c(DocID,Title),c("DocID","Title")))
    }

# A macro function for matching PBDB and DDRefs
macroTitles<-function(PBDBRefs,DDRefs) {    
    TemporaryMatches<-as.data.frame(t(apply(DDRefs,1,matchTitles,PBDBRefs)))
    return(TemporaryMatches[which.max(TemporaryMatches[,"Title"]),])
    }
    
# Establish a cluster for doParallel
# Make Core Cluster 
Cluster<-makeCluster(3)
# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("matchTitles","stringsim","macroTitles"))
MatchTitles<-parApply(Cluster, PBDBRefs, 1, macroTitles, DDRefs)

# Stop the cluster
stopCluster(Cluster)

# Convert PBDBReferences into a data frame
MatchTitles<-do.call(rbind,MatchReferences)
# Assign PBDB reference numbers as names to MatchReferencesList
rownames(MatchTitles)<-PBDBRefs[,"reference_no"]

# Determine how similar DDRef fields of the best DDRef title match are to each PBDB reference
#extract the best DeepDiveMatches
DDTitlesMatches<-DDRefs[which(DDRefs[,"reference_no"]%in%MatchTitles[,"DocID"]),]

# A function for matching PBDB and DDTitlesMatches
matchBibs<-function(Bib1,Bib2) {
    # Pub year match
    Year<-Bib1["pubyr"]==Bib2["pubyr"]
    # Journal Similarity
    Journal<-stringsim(Bib1["pubtitle"],Bib2["pubtitle"])
    # Author present
    Author<-grepl(Bib2["author"],Bib1["author"],perl=TRUE,ignore.case=TRUE)
    # Add docid column 
    DocID<-as.character(Bib1["reference_no"])
    # Return output     
    return(setNames(c(DocID,Year,Journal,Author),c("DocID","Year","Journal","Author")))
    }

# A macro function for matching PBDB and DDTitlesMatches
macroBibs<-function(PBDBRefs,DDTitlesMatches) {    
    TemporaryMatches<-as.data.frame(t(apply(DDTitlesMatches,1,matchBibs,PBDBRefs)))
    return(TemporaryMatches[which.max(TemporaryMatches[,"Title"]),])
    }
    
# Establish a cluster for doParallel
# Make Core Cluster 
Cluster<-makeCluster(3)
# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("matchBibs","stringsim","macroBibs"))
MatchReferences<-parApply(Cluster, PBDBRefs, 1, macroBibs, DDTitlesMatches)
