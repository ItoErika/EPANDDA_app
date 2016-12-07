# Custom functions are camelCase. Arrays, parameters, and arguments are PascalCase
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.

######################################### Load Required Libraries ###########################################
# Load Required Libraries
library("RPostgreSQL")
library("pbapply")
library("plyr")
library("stringdist")

Driver<-dbDriver("PostgreSQL") # Establish database driver
Panda<-dbConnect(Driver, dbname = "PANDA", host = "localhost", port = 5432, user = "zaffos")

#############################################################################################################
###################################### OUTPUT LOADING FUNCTIONS, PANDA ######################################
#############################################################################################################
# No functions at this time.
 
################################################ OUTPUT: Load Data ##########################################
# Upload the raw data from postgres
MatchReferences<-dbGetQuery(Panda, "SELECT * FROM panda_12052016.match_references;")

# Select the perfect matches
Perfect<-subset(MatchReferences,MatchReferences[,"title_sim"]==1 & MatchReferences[,"author_in"]==TRUE & MatchReferences[,"year_match"]==TRUE & MatchReferences[,"pubtitle_sim"]==1)
# Select the imperfect matches
Imperfect<-subset(MatchReferences,MatchReferences[,"pbdb_no"]%in%Perfect[,"pbdb_no"]!=TRUE)

