require(synapseClient)
synapseLogin()

## VARIABLES TO BE USED ACROSS TABLES
theseZips <- c("036", "692", "878", "059", "790", "879", "063", "821", "884", "102", "823", "890", "203", "830", "893", "556", "831")
outputProjId <- ""

## x IS EXPECTED TO BE A CHARACTER VECTOR TO BE CLEANED UP
cleanString <- function(x){
  gsub('[', '', gsub(']', '', gsub('"', '', x, fixed=T), fixed=T), fixed=T)
}

## x IS EXPECTED TO BE A LIST OF COLUMN MODEL OBJECTS
## ct IS THE COLUMN TYPE TO BE EXTRACTED
whichColumns <- function(x, ct){
  cc <- sapply(as.list(1:length(x)), function(y){
    if(x[[y]]@columnType==ct){
      return(x[[y]]@name)
    } else{
      return(NULL)
    }
  })
  cc <- unlist(cc)
  return(cc)
}

## synId IS THE SYNAPSE ID OF THE TABLE TO CLEAN UP
## ADD SUBSET TO ONLY THOSE WHO AGREED TO SHARE BROADLY
cleanTable <- function(synId){
  ## GET MAPPING OF SHARING STATUS
  sm <- synGet("syn7844939")
  shareMap <- read.delim(getFileLocation(sm), stringsAsFactors = FALSE)
  theseShared <- shareMap$healthCode[ shareMap$sharingScope == "all_qualified_researchers" ]
  
  ## GENERAL METADATA TO BE USED
  firstDate <- as.Date("2015-03-09")
  lastDate <- as.Date("2016-12-01")
  coreNames <- c("recordId", "healthCode", "createdOn", "appVersion", "phoneInfo")
  releaseVersions <- c("version 1.0, build 5",
                       "version 1.0.10, build 25",
                       "version 1.0.11, build 26",
                       "version 1.0.12, build 29",
                       "version 1.0.13, build 30",
                       "version 1.0.2, build 9",
                       "version 1.0.4, build 10",
                       "version 1.0.6, build 12",
                       "version 1.0.7, build 17",
                       "version 1.0.8, build 18",
                       "version 1.0.9, build 22",
                       "version 1.1.0, build 31, UK",
                       "version 1.1.0, build 31, US",
                       "version 1.2.0, build 33, UK",
                       "version 1.2.0, build 33, US",
                       "version 1.3.2, build 43, US",
                       "version 1.3.3, build 44, IE",
                       "version 1.3.3, build 44, UK",
                       "version 1.3.3, build 44, US",
                       "version 1.4, build 50, IE",
                       "version 1.4, build 50, UK",
                       "version 1.4, build 50, US",
                       "version 1.5, build 170, IE",
                       "version 1.5, build 170, UK",
                       "version 1.5, build 170, US",
                       "version 1.6, build 221, UK",
                       "version 1.6, build 221, US")
  
  ## GET ALL OF THE COLUMNS
  allCols <- synGetColumns(synId)@content
  stringCols <- whichColumns(allCols, "STRING")
  fhCols <- whichColumns(allCols, "FILEHANDLE")
  
  ## GET THE RAW DATA
  df <- synTableQuery(paste0("SELECT * FROM ", synId))@values
  df <- df[ which(df$healthCode %in% theseShared), ]
  
  ## DO SOME CLEANING
  for(i in stringCols){
    df[[i]] <- cleanString(df[[i]])
  }
  remCols <- c("externalId", "dataGroups", "uploadDate")
  df <- df[, -which(names(df) %in% remCols)]
  
  dfSub <- df[, setdiff(names(df), coreNames)]
  dfIdx <- rowSums(is.na(dfSub)) != ncol(dfSub)
  df <- df[ dfIdx, ]
  df <- df[ as.Date(df$createdOn) >= firstDate & as.Date(df$createdOn) <= lastDate, ]
  df <- df[ df$appVersion %in% releaseVersions, ]
  df <- df[ which(!duplicated(df[, c("healthCode", "createdOn")])), ]
  df[ order(df$createdOn), ]
  
  return(list(data=df, fhCols=fhCols))
}

#####
## CLEAN ALL OF THE DATA
#####
## Daily Prompt
dp1 <- cleanTable("syn3420229")
dp2 <- cleanTable("syn5652406")
for(cc in setdiff(names(dp2$data), names(dp1$data))){
  dp1$data[[cc]] <- NA
}
dailyPrompt <- list(data=rbind(dp1$data, dp2$data),
                    fhCols=dp2$fhCols)
rm(dp1, dp2, cc)
## NEED TO UNDERSTAND ENUMERATIONS FOR medicine, get_worse, get_worse_other

## Weedly Prompt
weeklyPrompt <- cleanTable("syn3633808")
## VERY SPARSE DATA

## Asthma History
asthmaHistory <- cleanTable("syn3420232")
## age_when_diagnosed

## Asthma Medication
am1 <- cleanTable("syn3420259")
am2 <- cleanTable("syn4910569")
am3 <- cleanTable("syn5663140")
for(cc in setdiff(names(am3$data), names(am1$data))){
  am1$data[[cc]] <- NA
}
for(cc in setdiff(names(am3$data), names(am2$data))){
  am2$data[[cc]] <- NA
}
asthmaMedication <- list(data=rbind(am1$data, am2$data, am3$data),
                         fhCols=am3$fhCols)
rm(am1, am2, am3, cc)
## non_adherent_other, quick_relief_other, other_meds

## Your Asthma
ya1 <- cleanTable("syn3420303")
ya2 <- cleanTable("syn5678648")
for(cc in setdiff(names(ya2$data), names(ya1$data))){
  ya1$data[[cc]] <- NA
}
yourAsthma <- list(data=rbind(ya1$data, ya2$data),
                   fhCols=ya2$fhCols)
rm(ya1, ya2, cc)
## asthma_gets_worse_with_other

## Medical History
mh1 <- cleanTable("syn3445112")
mh2 <- cleanTable("syn5678870")
for(cc in setdiff(names(mh2$data), names(mh1$data))){
  mh1$data[[cc]] <- NA
}
medicalHistory <- list(data=rbind(mh1$data, mh2$data),
                       fhCols=mh2$fhCols)
rm(mh1, mh2, cc)
## other_lung_disease_other

## About You
ay1 <- cleanTable("syn3454990")
ay2 <- cleanTable("syn5663141")
for(cc in setdiff(names(ay2$data), names(ay1$data))){
  ay1$data[[cc]] <- NA
}
aboutYou <- list(data=rbind(ay1$data, ay2$data),
                 fhCols=ay2$fhCols)
rm(ay1, ay2, cc)
## ethnicity, race, Income, education, health_insurance, race_other

## EQ-5D
eq5d <- cleanTable("syn3474924")
## pain, depression

## Non-Identifiable Demographics
demographics <- cleanTable("syn3917841")
## NonIdentifiableDemographics.json.patientCurrentAge

## Milestone
milestone <- cleanTable("syn4927134")
## height, weight, gender, age

## Air Quality Report
aq1 <- cleanTable("syn3420240")
aq2 <- cleanTable("syn4214143")
aq3 <- cleanTable("syn5607749")
aq4 <- cleanTable("syn5968321")
for(cc in setdiff(names(aq4$data), names(aq1$data))){
  aq1$data[[cc]] <- NA
}
for(cc in setdiff(names(aq4$data), names(aq2$data))){
  aq2$data[[cc]] <- NA
}
for(cc in setdiff(names(aq4$data), names(aq3$data))){
  aq3$data[[cc]] <- NA
}
airQualityReport <- list(data=rbind(aq1$data, aq2$data, aq3$data, aq4$data),
                         fhCols=aq4$fhCols)
rm(aq1, aq2, aq3, aq4, cc)
## REPORTING AREA AND STATE CODE -- MISSING LATER, BUT WITH OTHER RANDOM JSON FILES



## OTHER CODE
# pid <- "syn3270434"
# 
# qq <- synQuery(paste0('SELECT id, name FROM table WHERE parentId=="', pid, '"'))
# thisId <- qq$table.id[ grep('appVersion', qq$table.name)]
# 
# theseTables <- synTableQuery(paste0("SELECT DISTINCT originalTable FROM ", thisId))@values
# q <- qq[which(qq$table.name %in% theseTables$originalTable), ]
# 
# allDat <- lapply(q$table.id, function(x){
#   xx <- synTableQuery(paste0("SELECT * FROM ", x))@values
#   xx <- xx[ xx$appVersion %in% releaseVersions, ]
#   return(xx)
# })
# names(allDat) <- q$table.name
# cbind(t(sapply(allDat, dim)), q$table.id)


# aa <- do.call(rbind, allDat)
# aat <- table(aa$appVersion)
# aat <- aat[aat>100]
# aat <- aat[ grep("version 1.", names(aat), fixed=TRUE)]
# 
# sapply(allDat, nrow)
