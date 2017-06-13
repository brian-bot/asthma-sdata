require(synapseClient)
require(reshape2)
synapseLogin()

sharingFile <- synGet("syn7844939")
sf <- read.delim(getFileLocation(sharingFile), stringsAsFactors = FALSE)

# table(sf$status, sf$sharingScope)

zipFile <- synGet("syn9861625")
zf <- read.delim(getFileLocation(zipFile), stringsAsFactors = FALSE, check.names=FALSE)
zf$healthCode <- rownames(zf)
newDF <- melt(zf, id.vars="healthCode", na.rm=TRUE, value.name = "zip3")
newDF$zipDate <- as.character(newDF$variable)
newDF$zipDate <- gsub(".", "-", newDF$zipDate, fixed=TRUE)
newDF$variable <- NULL
newDF <- newDF[, c("healthCode", "zipDate", "zip3")]
newDF <- newDF[ order(newDF$zipDate, newDF$healthCode), ]
rownames(newDF) <- NULL

## REMOVE CODES BEFORE VERSION LAUNCH DATE OF 2015-05-05
newDF <- newDF[ newDF$zipDate >= "2015-05-05", ]
newDF <- newDF[ newDF$zip3 != "outside US", ]

## THESE ZIP CODES NEED TO BE CONSOLIDATED
theseZips <- c("036", "692", "878", "059", "790", "879", "063", "821", "884", "102", "823", "890", "203", "830", "893", "556", "831")

newDF$zip3[ newDF$zip3 %in% theseZips ] <- "000"

## MAKE SURE WE HAVE THE RIGHT SUBSET
newDF <- newDF[ newDF$healthCode %in% sf$healthCode[ sf$sharingScope == "all_qualified_researchers" ], ]

tcs <- as.tableColumns(newDF)
tcs$tableColumns[[2]]@columnType <- "STRING"
tcs$tableColumns[[2]]@maximumSize <- 10

finalTable <- synStore(
  Table(
    tableSchema = TableSchema(name="Participant 3 Digit Zip", parent = "syn8361748", columns = tcs$tableColumns), 
    values=tcs$fileHandleId
  )
)
