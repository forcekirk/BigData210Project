// Databricks notebook source
//Read, combine and save files as Parquet format
val subRDD = spark.read.option("inferSchema", "true").option("header", "true").option("sep", "\t").csv("dbfs:/autumn_2019/kforce/FinalProject/sub")
val tagRDD = spark.read.option("inferSchema", "true").option("header", "true").option("sep", "\t").csv("dbfs:/autumn_2019/kforce/FinalProject/tag")
val numRDD = spark.read.option("inferSchema", "true").option("header", "true").option("sep", "\t").csv("dbfs:/autumn_2019/kforce/FinalProject/num")
val preRDD = spark.read.option("inferSchema", "true").option("header", "true").option("sep", "\t").csv("dbfs:/autumn_2019/kforce/FinalProject/pre")
subRDD.write.parquet("dbfs:/autumn_2019/kforce/FinalProject/subparquet")
tagRDD.write.parquet("dbfs:/autumn_2019/kforce/FinalProject/tagparquet")
numRDD.write.parquet("dbfs:/autumn_2019/kforce/FinalProject/numparquet")
preRDD.write.parquet("dbfs:/autumn_2019/kforce/FinalProject/preparquet")

// COMMAND ----------

//Load numeric data
val numParquet = spark.read.parquet("dbfs:/autumn_2019/kforce/FinalProject/numparquet")
val numNum = numParquet.count


// COMMAND ----------

//Load submission data
val subParquet = spark.read.parquet("dbfs:/autumn_2019/kforce/FinalProject/subparquet")

//Number of submissions (total and quarter)
val numSubmissions = subParquet.count
val subByQuarter = subParquet.groupBy($"period")
val numOfSubByQuarter = subByQuarter.count


// COMMAND ----------

//Join numeric and submission data by sumission
val joinedSubNum = numParquet.as("num").join(subParquet.as("sub"), $"num.adsh" === $"sub.adsh") //&& $"num.ddate" === $"sub.period")

//Num entries per quarter
val numByPeriod = joinedSubNum.groupBy("sub.period")
val countNumByPeriod = numByPeriod.count


// COMMAND ----------

////////////////////
//Narrow down to only single quarter data data in 10K or 10Q forms)
val quarterlySubNum = joinedSubNum.filter($"num.qtrs" === 1 && ($"sub.form" === "10-K" || $"sub.form" === "10-Q"))

//In order to capture reinstatements we have to find the max submission date for each CIK, ddate, and tag combination
val mostRecentTagGroup = quarterlySubNum.groupBy($"sub.cik", $"num.tag", $"num.ddate")
val mostRecentTagMaxDate = mostRecentTagGroup.max("sub.period")

//Join numeric/submission data on max submission date 
val mostRecentSubNum = quarterlySubNum.as("subnum").join(mostRecentTagMaxDate.as("recent"), 
                                                   $"subnum.ddate" === $"recent.ddate" && 
                                                   $"subnum.period" === $"recent.max(period)" && 
                                                   $"subnum.tag" === $"recent.tag" && 
                                                   $"subnum.cik" === $"recent.cik")

val finalCols = Seq("subnum.cik", "name", "subnum.tag", "subnum.ddate", "value", "period", "footnote")
val finalData = mostRecentSubNum.select(finalCols.head, finalCols.tail: _*)
finalData.write.parquet("dbfs:/autumn_2019/kforce/FinalProject/finalset")

// COMMAND ----------

////////////////////
//Narrow down to only yearly data in 10K or 10Q forms)
val yearlySubNum = joinedSubNum.filter($"num.qtrs" === 4 && $"fp" ==="FY" && ($"sub.form" === "10-K" || $"sub.form" === "10-Q"))

//In order to capture reinstatements we have to find the max submission date for each CIK, ddate, and tag combination
val mostRecentTagGroupYear = yearlySubNum.groupBy($"sub.cik", $"num.tag", $"num.ddate")
val mostRecentTagMaxDateYear = mostRecentTagGroupYear.max("sub.period")

//Join numeric/submission data on max submission date 
val mostRecentSubNumYear = yearlySubNum.as("subnum").join(mostRecentTagMaxDateYear.as("recent"), 
                                                   $"subnum.ddate" === $"recent.ddate" && 
                                                   $"subnum.period" === $"recent.max(period)" && 
                                                   $"subnum.tag" === $"recent.tag" && 
                                                   $"subnum.cik" === $"recent.cik")

val finalColsYear = Seq("subnum.cik", "name", "subnum.tag", "subnum.ddate", "value", "period", "footnote")
val finalDataYear = mostRecentSubNumYear.select(finalCols.head, finalCols.tail: _*)
finalDataYear.write.parquet("dbfs:/autumn_2019/kforce/FinalProject/finalsetyear")

// COMMAND ----------

//Example: Microsoft net income
val subMSFT = subParquet.filter($"name".contains("MICROSOFT")).select($"cik").distinct.collect
val msftCIK = subMSFT(0).get(0)

//Example CIKs AAPL: 0000320193; MSFT: 0000789019; IBM: 0000051143;
//val msftCIK = "0000051143"
val colNames = Seq("subnum.ddate", "subnum.period", "subnum.value")
val msftNI = mostRecentSubNum.filter($"subnum.cik" === msftCIK && ($"subnum.tag" === "NetIncomeLoss" || $"subnum.tag" === "ProfitLoss")).select(colNames.head, colNames.tail: _*)
display(msftNI)


// COMMAND ----------

//Tag data
val tagParquet = spark.read.parquet("dbfs:/autumn_2019/kforce/FinalProject/tagparquet")

val gaapTags =tagParquet.filter($"version".contains("gaap"))
val countUniqueTags = tagParquet.distinct.count
val countGAAPTags = gaapTags.count

//Attempt to narrow down revenue measures
//val revMeasures = tagParquet.filter($"tag".contains("Revenue") && $"version".contains("gaap") && $"version".contains("2018") && $"abstract" === 0)
//val revDF = revMeasures.toDF()
//display(revDF)

// COMMAND ----------

//Load presentation data
val preParquet = spark.read.parquet("dbfs:/autumn_2019/kforce/FinalProject/preparquet")


// COMMAND ----------

//Ex: Microsoft  Q1 2019 income statement
val msftIS = preParquet.filter($"adsh" === "0001564590-19-012709" && $"stmt" === "IS")
val msftFullIS = msftIS.as("is").join(numParquet.as("num"), $"num.adsh" === $"is.adsh" && $"num.tag" === $"is.tag")
val msftQuarterValues = msftFullIS.filter($"num.ddate" === "20190331" && $"num.qtrs" === 1)

val showCols = Seq("is.line", "is.plabel", "num.value")
display(msftQuarterValues.select(showCols.head, showCols.tail: _*))

// COMMAND ----------

//Experimentation. Disregard
//val cik = "1675149"
//val period = "20151231"
//val adshArr = subParquet.filter($"cik" === cik && $"period" === period).select("adsh").distinct.as[String].collect

//val adsh = adshArr(0)
//val IS = preParquet.filter($"adsh" === adsh && $"stmt" === "IS")
//val FullIS = IS.as("is").join(numParquet.as("num"), $"num.adsh" === $"is.adsh" && $"num.tag" === $"is.tag")
//val QuarterValues = FullIS.filter($"num.qtrs" === 1)

//display(FullIS)


// COMMAND ----------

//Exploring Dow data. Disregard
//val dowMembers = spark.read.option("header", "true").option("sep", ",").csv("dbfs:/autumn_2019/kforce/FinalProject/DOWMembers.csv")
//val dowSubmissions = dowMembers.as("dow").join(subParquet.as("sub"), $"sub.cik" === $"dow.CIK")
//val dowSubCols = Seq("dow.Name", "dow.CIK", "sub.adsh", "sub.period")
//val dowSub = dowSubmissions.select(dowSubCols.head, dowSubCols.tail: _*)
//val dowPre = dowSub.as("dow").join(parquetPre.as("pre"), $"dow.adsh" === $"pre.adsh")

//import org.apache.spark.sql.functions.lower
//val dowRevenue = dowPre.filter(lower($"tag").contains("revenue"))
//display(dowRevenue)
//val showCols = Seq("dow.Name", "dow.CIK", "dow.period", "pre.line", "pre.plabel")
//val dowFilt = dowRevenue.select(showCols.head, showCols.tail: _*)


// COMMAND ----------


