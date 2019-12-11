# Databricks notebook source
library(SparkR)
install.packages("xts")
install.packages("quantmod")
library(quantmod)
library(xts)

data <- read.df("dbfs:/autumn_2019/kforce/FinalProject/finalset")
dataYear <- read.df("dbfs:/autumn_2019/kforce/FinalProject/finalsetyear")
dowMembers <-  read.df("dbfs:/autumn_2019/kforce/FinalProject/DOWMembers.csv", source = "csv", header="true", inferSchema = "true")

# COMMAND ----------

#Join datasets by Dow members. At this point the data is small enough to convert to a regular R data frame
dowQuarter <- collect(merge(data, dowMembers, by.x = "cik", by.y = "CIK"))
dowYear <- collect(merge(dataYear, dowMembers,  by.x = "cik", by.y = "CIK"))
dow.members <- collect(dowMembers)


# COMMAND ----------

library(lubridate)

#Convert ddate to date class
dowQuarter$ddate <- as.Date(as.character(dowQuarter$ddate), format = "%Y%m%d")
dowYear$ddate <- as.Date(as.character(dowYear$ddate), format = "%Y%m%d")
dow.members$'Start Date' <- as.Date(dow.members$'Start Date', "%m/%d/%Y")
dow.members$'End Date' <- as.Date(dow.members$'End Date', "%m/%d/%Y")

EoMonth <- function(date, offset = 0, format = "%Y-%m-%d"){
  #Finds the end of a month associated with a date and an offset
  #Args: date: The base date for comparison. offset: the number of months 
  #to offset from date to find month end. Ex: EoMonth("2010-02-21, -1) will
  #return "2010-01-31". format: format of the string given
  #Returns: The month end date given date and offset
  
  #If date is not a date object, convert it
  date <- as.Date(date, format)
  
  #Subtract to first of the month
  date <- date - (day(date) - 1)
  
  #Add necessary offset in months
  date <- date %m+% months(offset + 1) - 1
  
  return(date)
}
                       
roundDatesToQuarter <- function(date_vec){
  #Converts a vector of dates to the closest quarter ends
  offset <- ifelse(month(date_vec)%%3 != 0, 3 - month(date_vec)%%3, 0)
  EoMonth(date_vec, offset)
}

quarterSeq <- function(start.date, end.date){
  #Generates a sequence between two quarter ends
  q.seq <- start.date
  i <- 1
  while(q.seq[i] < end.date){
    i <- i + 1
    q.seq <- c(q.seq, EoMonth(q.seq[i-1], 3))
  }
  return(q.seq)
}

#Round the dates to the nearest quarter
dowQuarter$ddate <- roundDatesToQuarter(dowQuarter$ddate)
dowYear$ddate <- roundDatesToQuarter(dowYear$ddate)



# COMMAND ----------

getDowMembers <- function(period.date, dow.members){
  which.memb <- which(dow.members$'Start Date' <= period.date & dow.members$'End Date' >= period.date)
  return(dow.members[which.memb ,])
}
dow.members[which(dow.members$CIK == "732717"), 4] <- as.Date("2015-03-19")


# COMMAND ----------

getRevenues <- function(cik, quarter.data, year.data){
  #Returns the revenues for the provided company ID number 
  

  #Attempt to get quarterly revenues in order of common tags
  cik.quarter <- quarter.data[which(quarter.data$cik == cik) ,]
  rev.try.order <- c("Revenues", "SalesRevenueNet", "SalesRevenueServicesNet", "SalesRevenueGoodsNet", "RevenuesNetOfInterestExpense", 
                     "RevenueFromContractWithCustomerExcludingAssessedTax", "TotalRevenuesNetOfInterestExpense", "TotalRevenuesAndOtherIncome")
  rev.df <- NA
  for (i in 1:length(rev.try.order)){
    rev.tag <- rev.try.order[i]
    
    #Search for match to revenue tag
    rev.subset <- cik.quarter[which(cik.quarter$tag == rev.tag) ,]
    
    #If a match is found, add it to the revenue data frame
    if (nrow(rev.subset) > 0){
      if(is.na(rev.df)){
        rev.df <- rev.subset
      } else {
        #Determine dates that haven't already been found
        which.new <- which(!(rev.subset$ddate %in% rev.df$ddate))
        if (length(which.new) > 0){
          rev.df <- rbind(rev.df, rev.subset[which.new ,])
        }
      }
    }
  }
  
  #Search for any missing dates in the quarterly data set
  min.date <- min(rev.df$ddate)
  max.date <- max(rev.df$ddate)
  date.seq <- quarterSeq(min.date, max.date)
  missing.dates <- date.seq[!(date.seq %in% rev.df$ddate)]
  
  #If there are missing dates, attempt to derive quarterly values from the annual values
  if (length(missing.dates) >  0){
    cik.year <- year.data[which(year.data$cik == cik) ,]
    for (i in 1:length(missing.dates)){
      curr.date <- missing.dates[i]
  
      #Check if there are 3 previous quarterly values
      prev.q <- EoMonth(curr.date, -c(9, 6, 3))
      prev.q.data <- rev.df[which(rev.df$ddate %in% prev.q) ,]

      if (nrow(prev.q.data) == 3){
        #Subset annual values and search for revenue tag order like above
        ann.vals <- cik.year[which(cik.year$ddate == curr.date) ,]
        found.ann <- F
        if(!found.ann){
          for (i in 1:length(rev.try.order)){
            rev.tag <- rev.try.order[i]
            rev.subset <- ann.vals[which(ann.vals$tag == rev.tag) ,]
            if (nrow(rev.subset) > 0){
              found.ann <- T
              q.val <- rev.subset$value[1] - sum(prev.q.data$value)
              new.df <- rev.subset[1 ,]
              new.df$value <- q.val
              rev.df <- rbind(rev.df, new.df)
            }
          }
        }
      }
    }
  
}
  
  return(rev.df)
}


ciks <- collect(distinct(select(dowMembers, "cik")))[, 1]
rev <- getRevenues(ciks[1], dowQuarter, dowYear)
for (i in 2:length(ciks)){
  cik <- ciks[i]
  rev <- rbind(rev, getRevenues(cik, dowQuarter, dowYear))
}

# COMMAND ----------

#Calculate aggregated revenues
q.seq <- quarterSeq(as.Date("2011-09-30"), as.Date("2019-06-30"))

calculateDowRevenue <- function(rev.df, member.df, period){
  period.members <- getDowMembers(period, member.df)
  period.rev <- rev.df[which(rev.df$ddate == period) ,]
  member.rev <- merge(period.rev, period.members, by.x = "cik", by.y = "CIK")
  return(sum(member.rev$value))
}

total.rev <- data.frame(q.seq, rep(NA, length(q.seq)))
for (i in 1:length(q.seq)){
  total.rev[i, 2] <- calculateDowRevenue(rev, dow.members, q.seq[i])
}

display(total.rev)
plot(total.rev, type = "b", pch = 19, ylab = "Revenue ($)", main = "DJIA Aggregate GAAP Revenue Q3 2011 - Q3 2019", 
    col = "cornflowerblue", xlab = "")
grid()

# COMMAND ----------

#Plot dow prices for comparison
djia <- getSymbols("DJIA", auto.assign = F)
dow.close <- Cl(djia)
plot(dow.close["2011-09-30/2019-06-30"], main = "DJIA Closing Value", yaxis.right = F, major.format = "%Y")

# COMMAND ----------

#Microsoft net income
msft.cik <- "789019"
msft.data <- dowQuarter[which(dowQuarter$CIK == msft.cik) ,]
msft.ni <- msft.data[which(msft.data$tag == "NetIncomeLoss") ,]
msft.ni.xts <- xts(msft.ni$value, msft.ni$ddate)

plot(msft.ni.xts["2011-09-30/"], main = "Microsoft GAAP Net Income", col = "blue", type = "h", yaxis.right = F)


# COMMAND ----------

#Microsoft stock price
msft <- getSymbols("MSFT", auto.assign = F)
msft.close <- Cl(msft)
plot(msft.close["2011-09-30/2019-06-30"], main = "MSFT Closing Value", yaxis.right = F, major.format = "%Y")

# COMMAND ----------

#AAPL net income
aapl.cik <- "320193"
aapl.data <- dowQuarter[which(dowQuarter$CIK == aapl.cik) ,]
aapl.ni <- aapl.data[which(aapl.data$tag == "NetIncomeLoss") ,]
aapl.ni.xts <- xts(aapl.ni$value, aapl.ni$ddate)

plot(aapl.ni.xts["2011-09-30/"], main = "Apple GAAP Net Income", col = "red", type = "h", yaxis.right = F)


# COMMAND ----------

aapl <- getSymbols("AAPL", auto.assign = F)
aapl.close <- Cl(aapl)
plot(aapl.close["2011-09-30/2019-06-30"], main = "AAPL Closing Value", yaxis.right = F, major.format = "%Y")

# COMMAND ----------


