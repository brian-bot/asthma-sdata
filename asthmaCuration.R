require(synapseClient)
synapseLogin()

## VARIABLES TO BE USED ACROSS TABLES
theseZips <- c("036", "692", "878", "059", "790", "879", "063", "821", "884", "102", "823", "890", "203", "830", "893", "556", "831")
outputProjId <- "syn8361748"

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
  ## DO NOT KEEP ANY WHICH COLLECTED INTERNATIONAL DATA
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
                       "version 1.1.0, build 31, US",
                       "version 1.2.0, build 33, US",
                       "version 1.3.2, build 43, US",
                       "version 1.3.3, build 44, US",
                       "version 1.4, build 50, US",
                       "version 1.5, build 170, US",
                       "version 1.6, build 221, US")
  
  ## GET ALL OF THE COLUMNS
  allCols <- synGetColumns(synId)@content
  stringCols <- whichColumns(allCols, "STRING")
  fhCols <- whichColumns(allCols, "FILEHANDLEID")
  
  ## GET THE RAW DATA
  df <- synTableQuery(paste0("SELECT * FROM ", synId))@values
  df <- df[ which(df$healthCode %in% theseShared), ]
  
  ## DO SOME CLEANING
  for(i in stringCols){
    df[[i]] <- cleanString(df[[i]])
  }
  ## REMOVE A FEW COLUMNS WHICH ARE NOT RELEVANT
  remCols <- c("externalId", "dataGroups", "uploadDate")
  df <- df[, -which(names(df) %in% remCols)]
  
  ## LOOK FOR COMPLETE DUPLICATES
  dfSub <- df[, setdiff(names(df), coreNames)]
  dfIdx <- rowSums(is.na(dfSub)) != ncol(dfSub)
  df <- df[ dfIdx, ]
  ## SUBSET BASED ON DATE AND APP VERSION
  df <- df[ as.Date(df$createdOn) >= firstDate & as.Date(df$createdOn) <= lastDate, ]
  df <- df[ df$appVersion %in% releaseVersions, ]
  ## REMOVE DUPS WHICH HAVE SAME CREATEDON TIMESTAMP (LIKELY EXPORT ERROR)
  df <- df[ order(df$healthCode, df$createdOn), ]
  df <- df[ which(!duplicated(df[, c("healthCode", "createdOn")])), ]
  df <- df[ order(df$createdOn), ]
  
  return(list(data=df, fhCols=fhCols))
}

#########################
## CLEAN ALL OF THE DATA
#########################

#####
## Daily Prompt
#####
dp1 <- cleanTable("syn3420229")
dp2 <- cleanTable("syn5652406")
for(cc in setdiff(names(dp2$data), names(dp1$data))){
  dp1$data[[cc]] <- NA
}
dp <- rbind(dp1$data, dp2$data)
dp$any_activity <- NULL
dp$get_worse_other <- NULL
## COLLAPSE SAME-DAY RESPONSES - ANY ANSWER OF TRUE TO BE INCLUDED
dp$day <- as.Date(dp$createdOn)
dp <- dp[ order(dp$healthCode, dp$createdOn), ]
idx <- which(duplicated(dp[, c("healthCode", "day")]))
dptmp <- dp[ idx, c("healthCode", "day")]
for(i in 1:nrow(dptmp)){
  tmpIdx <- which(dp$healthCode==dptmp$healthCode[i] & dp$day==dptmp$day[i])
  dp$medicine[tmpIdx] <- min(dp$medicine[tmpIdx])
  dp$medicine_change[tmpIdx] <- any(dp$medicine_change[tmpIdx], na.rm = TRUE)
  dp$day_symptoms[tmpIdx] <- any(dp$day_symptoms[tmpIdx], na.rm = TRUE)
  dp$night_symptoms[tmpIdx] <- any(dp$night_symptoms[tmpIdx], na.rm = TRUE)
  dp$use_qr[tmpIdx] <- any(dp$use_qr[tmpIdx], na.rm = TRUE)
  if( !all(is.na(dp$quick_relief_puffs[tmpIdx]))){
    dp$quick_relief_puffs[tmpIdx] <- max(dp$quick_relief_puffs[tmpIdx], na.rm = TRUE)
  }
  dp$get_worse[tmpIdx] <- paste(unique(unlist(strsplit(dp$get_worse[tmpIdx], ","))), collapse = ",")
  if( !all(is.na(dp$peakflow[tmpIdx]))){
    dp$peakflow[tmpIdx] <- max(dp$peakflow[tmpIdx], na.rm = TRUE)
  }
}
dp$day <- NULL
dp <- dp[-idx, ]
dailyPrompt <- list(data=dp,
                    fhCols=NULL) ## ONLY FHCOL WAS 'OTHER' WHICH WAS REMOVED
rm(dp1, dp2, cc, dp, idx, i, tmpIdx, dptmp)

#####
## Weekly Prompt
#####
weeklyPrompt <- cleanTable("syn3633808")
wp <- weeklyPrompt$data
wp$oral_steroids_when <- as.Date(wp$oral_steroids_when)
wp$prednisone_when <- as.Date(wp$prednisone_when)
wp$er_when <- as.Date(wp$er_when)
wp$admitted_when <- as.Date(wp$admitted_when)
wp$oral_steroids_when.timezone <- NULL
wp$prednisone_when.timezone <- NULL
wp$er_when.timezone <- NULL
wp$admitted_when.timezone <- NULL
## COLLAPSE SAME-DAY RESPONSES - ANY ANSWER OF TRUE TO BE INCLUDED
wp$day <- as.Date(wp$createdOn)
wp <- wp[ order(wp$healthCode, wp$day), ]
idx <- which(duplicated(wp[, c("healthCode", "day")]))
wptmp <- wp[ idx, c("healthCode", "day")]
for(i in 1:nrow(wptmp)){
  tmpIdx <- which(wp$healthCode==wptmp$healthCode[i] & wp$day==wptmp$day[i])
  wp$asthma_doc_visit[tmpIdx] <- any(wp$asthma_doc_visit[tmpIdx], na.rm = TRUE)
  wp$asthma_medicine[tmpIdx] <- any(wp$asthma_medicine[tmpIdx], na.rm = TRUE)
  wp$oral_steroids[tmpIdx] <- any(wp$oral_steroids[tmpIdx], na.rm = TRUE)
  if( !all(is.na(wp$oral_steroids_when[tmpIdx]))){
    wp$oral_steroids_when[tmpIdx] <- min(wp$oral_steroids_when[tmpIdx], na.rm = TRUE)
  }
  wp$prednisone[tmpIdx] <- any(wp$prednisone[tmpIdx], na.rm = TRUE)
  if( !all(is.na(wp$prednisone_when[tmpIdx]))){
    wp$prednisone_when[tmpIdx] <- min(wp$prednisone_when[tmpIdx], na.rm = TRUE)
  }
  wp$emergency_room[tmpIdx] <- any(wp$emergency_room[tmpIdx], na.rm = TRUE)
  if( !all(is.na(wp$er_when[tmpIdx]))){
    wp$er_when[tmpIdx] <- min(wp$er_when[tmpIdx], na.rm = TRUE)
  }
  wp$admission[tmpIdx] <- any(wp$admission[tmpIdx], na.rm = TRUE)
  if( !all(is.na(wp$admitted_when[tmpIdx]))){
    wp$admitted_when[tmpIdx] <- min(wp$admitted_when[tmpIdx], na.rm = TRUE)
  }
  wp$admitted_end[tmpIdx] <- paste(unique(unlist(strsplit(wp$admitted_end[tmpIdx], ","))), collapse = ",")
  wp$limitations[tmpIdx] <- any(wp$limitations[tmpIdx], na.rm = TRUE)
  if( !all(is.na(wp$limitations_days[tmpIdx]))){
    wp$limitations_days[tmpIdx] <- max(wp$limitations_days[tmpIdx], na.rm = TRUE)
  }
  wp$missed_work[tmpIdx] <- any(wp$missed_work[tmpIdx], na.rm = TRUE)
  wp$missed_work_days[tmpIdx] <- paste(unique(unlist(strsplit(wp$missed_work_days[tmpIdx], ","))), collapse = ",")
  if( !all(is.na(wp$side_effects[tmpIdx]))){
    wp$side_effects[tmpIdx] <- max(wp$side_effects[tmpIdx], na.rm = TRUE)
  }
}
wp$day <- NULL
wp <- wp[-idx, ]
weeklyPrompt$data <- wp
rm(wp, idx, i, tmpIdx, wptmp)

#####
## Asthma History
#####
asthmaHistory <- cleanTable("syn3420232")
ah <- asthmaHistory$data
## KEEP FIRST SURVEY
ah <- ah[ order(ah$healthCode, ah$createdOn), ]
ah <- ah[ -which(duplicated(ah[, c("healthCode")])), ]
asthmaHistory$data <- ah
rm(ah)
## IS age_when_diagnosed OK?

#####
## Asthma Medication
#####
am1 <- cleanTable("syn3420259")
am2 <- cleanTable("syn4910569")
am3 <- cleanTable("syn5663140")
for(cc in setdiff(names(am3$data), names(am1$data))){
  am1$data[[cc]] <- NA
}
for(cc in setdiff(names(am3$data), names(am2$data))){
  am2$data[[cc]] <- NA
}
am <- rbind(am1$data, am2$data, am3$data)
## REMOVE FREE TEXT FIELDS
am$non_adherent_other <- NULL
am$quick_relief_other <- NULL
am$other_meds <- NULL
## KEEP FIRST SURVEY
am <- am[ order(am$healthCode, am$createdOn), ]
am <- am[ -which(duplicated(am[, c("healthCode")])), ]
asthmaMedication <- list(data=am,
                         fhCols=NULL) ## fhCols were all 'other' columns which are excluded
rm(am1, am2, am3, am, cc)

#####
## Your Asthma
#####
ya1 <- cleanTable("syn3420303")
ya2 <- cleanTable("syn5678648")
for(cc in setdiff(names(ya2$data), names(ya1$data))){
  ya1$data[[cc]] <- NA
}
ya <- rbind(ya1$data, ya2$data)
ya$asthma_gets_worse_with_other <- NULL
## KEEP FIRST SURVEY
ya <- ya[ order(ya$healthCode, ya$createdOn), ]
ya <- ya[ -which(duplicated(ya[, c("healthCode")])), ]
yourAsthma <- list(data=ya,
                   fhCols=NULL)
rm(ya1, ya2, ya, cc)

#####
## Medical History
#####
mh1 <- cleanTable("syn3445112")
mh2 <- cleanTable("syn5678870")
for(cc in setdiff(names(mh2$data), names(mh1$data))){
  mh1$data[[cc]] <- NA
}
mh <- rbind(mh1$data, mh2$data)
mh$other_lung_disease_other <- NULL
## KEEP FIRST SURVEY
mh <- mh[ order(mh$healthCode, mh$createdOn), ]
mh <- mh[ -which(duplicated(mh[, c("healthCode")])), ]
medicalHistory <- list(data=mh,
                       fhCols=NULL)
rm(mh1, mh2, mh, cc)
## CHECK THESE WITH GOVERNANCE TEAM

#####
## About You
#####
ay1 <- cleanTable("syn3454990")
ay2 <- cleanTable("syn5663141")
for(cc in setdiff(names(ay2$data), names(ay1$data))){
  ay1$data[[cc]] <- NA
}
ay <- rbind(ay1$data, ay2$data)
ay$race_other <- NULL
## KEEP FIRST SURVEY
ay <- ay[ order(ay$healthCode, ay$createdOn), ]
ay <- ay[ -which(duplicated(ay[, c("healthCode")])), ]
aboutYou <- list(data=ay,
                 fhCols=NULL)
rm(ay1, ay2, ay, cc)
## CHECK THESE WITH GOVERNANCE TEAM

#####
## EQ-5D
#####
eq5d <- cleanTable("syn3474924")
eq5d$data$intro <- NULL
eq5d$data$EQ5Instructions <- NULL
eq5d$data$slider_instructions <- NULL

#####
## Non-Identifiable Demographics
#####
demographics <- cleanTable("syn3917841")
dd <- demographics$data
dd$weightPounds <- dd$NonIdentifiableDemographics.json.patientWeightPounds
dd$NonIdentifiableDemographics.json.patientWeightPounds <- NULL
dd$biologicalSex <- dd$NonIdentifiableDemographics.json.patientBiologicalSex
dd$NonIdentifiableDemographics.json.patientBiologicalSex <- NULL
dd$heightInches <- dd$NonIdentifiableDemographics.json.patientHeightInches
dd$NonIdentifiableDemographics.json.patientHeightInches <- NULL
dd$age <- dd$NonIdentifiableDemographics.json.patientCurrentAge
dd$NonIdentifiableDemographics.json.patientCurrentAge <- NULL
dd$NonIdentifiableDemographics.json.patientWakeUpTime <- NULL
dd$NonIdentifiableDemographics.json.patientGoSleepTime <- NULL
dd$NonIdentifiableDemographics.json.item <- NULL
## KEEP FIRST SURVEY
dd <- dd[ order(dd$healthCode, dd$createdOn), ]
dd <- dd[ -which(duplicated(dd[, c("healthCode")])), ]
dd <- dd[ -which(dd$age < 18), ]
demographics$data <- dd
rm(dd)

#####
## Milestone
#####
milestone <- cleanTable("syn4927134")
mm <- milestone$data
## KEEP FIRST SURVEY
mm <- mm[ order(mm$healthCode, mm$createdOn), ]
mm <- mm[ -which(duplicated(mm[, c("healthCode")])), ]
milestone$data <- mm
rm(mm)

#####
## Air Quality Report
#####
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
aq <- rbind(aq1$data, aq2$data, aq3$data, aq4$data)
aq$aqiResponse.json.reporting_area <- NULL
aq$aqiResponse.json.state_code <- NULL
aq$latlong.json <- NULL
aq$pollen.json <- NULL
aq$resourceparams.json <- NULL
aq$mapOverlay.png <- NULL

airQualityReport <- list(data=aq,
                         fhCols=c("aqiResponse.json.reports", "breezometer"))
rm(aq1, aq2, aq3, aq4, aq, cc)
## SHOULD LEARN THE DIFFERENCE BETWEEN THE AQI AND BREEZOMETER FILES

## LOG IN AS BRIDGE EXPORTER TO UPLOAD TABLES WITH FILE HANDLES OWNED BY THAT USER
storeThese <- list('About You Survey' = aboutYou,
                   'Air Quality Reports' = airQualityReport,
                   'Asthma History Survey' = asthmaHistory,
                   'Asthma Medication Survey' = asthmaMedication,
                   'Daily Prompt Survey' = dailyPrompt,
                   'Demographics Survey' = demographics,
                   'EQ5D Survey' = eq5d,
                   'Medical History Survey' = medicalHistory,
                   'Milestone Survey' = milestone,
                   'Weekly Prompt Survey' = weeklyPrompt,
                   'Your Asthma Survey' = yourAsthma)

storeThese <- lapply(storeThese, function(tt){
  ## USE TAB DELIMITED FOR CASES WHERE COMMAS ARE USED IN TEXT FIELDS
  tcs <- as.tableColumns(tt$data)
  for(i in 1:length(tcs$tableColumns)){
    ## NAMES IN tcs HAVE . REMOVED - NEED TO KEEP CONSISTENT
    tcs$tableColumns[[i]]@name <- names(tt$data)[i]
    if(tcs$tableColumns[[i]]@name %in% tt$fhCols){
      tcs$tableColumns[[i]]@columnType <- "FILEHANDLEID"
      tcs$tableColumns[[i]]@defaultValue <- character(0)
      tcs$tableColumns[[i]]@maximumSize <- integer(0)
      tcs$tableColumns[[i]]@enumValues <- character(0)
    }
  }
  return(tcs)
})

## FINALLY, STORE THE OUTPUT
for(i in length(storeThese):1){
  theEnd <- synStore(Table(TableSchema(name=names(storeThese)[i],
                                       parent=outputProjId,
                                       columns=storeThese[[i]]$tableColumns),
                           values = storeThese[[i]]$fileHandleId))
}
