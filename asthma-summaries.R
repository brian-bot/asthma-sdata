require(synapseClient)
synapseLogin()

q <- synQuery('SELECT id, name FROM table WHERE parentId=="syn8361748"')

q$participants <- NA
q$records <- NA
for(i in 1:nrow(q)){
  tq <- synTableQuery(paste0('SELECT healthCode FROM ', q$table.id[i]))@values
  q$participants[i] <- length(unique(tq$healthCode))
  q$records[i] <- nrow(tq)
}

