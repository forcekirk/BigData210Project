#Download data from Edgar, unzip into component parts

data.folder.path <- "C:/Users/kforce/Desktop/Big Data 210/Final Project/Data"

#Construct quarter/year combinations to request
quarters <- paste("q", seq(1, 4), sep = "")
years <- seq(2012, 2019)
dates <- c()
for (y in years){
  for (q in quarters){
    dates <- c(dates, paste(y, q, sep = ""))
  }
}
download.quarters <- c("2011q3", "2011q4", dates[-length(dates)])

#Download files from SEC website
base.url <- "https://www.sec.gov/files/dera/data/financial-statement-data-sets/"
for (i in 1:length(download.quarters)){
  file.url <- paste(base.url, download.quarters[i], ".zip", sep = "")
  file.dest.path <- paste(data.folder.path, "/", download.quarters[i], ".zip", sep = "")
  download.file(url = file.url, destfile = file.dest.path)
}

#Unzip and sort files into their component directories with date identifiers
file.prefix <- c("num", "pre", "sub", "tag")
pre.file.names <- paste(file.prefix, ".txt", sep = "")
for (i in 1:length(download.quarters)){
  zip.path <- paste(data.folder.path, "/", download.quarters[i], ".zip", sep = "")
  extract.to <-  paste(data.folder.path, "/", download.quarters[i], sep = "")
  dir.create(extract.to)
  unzip(zip.path, exdir = extract.to)
  

  post.file.names <- paste(download.quarters[i], pre.file.names, sep = "")
  file.copy(from = paste(extract.to, pre.file.names, sep = "/"), 
            to = paste(data.folder.path, "/", file.prefix, "/", post.file.names, sep = ""))
  
}


