# Big Data 210 Final Project: Exploring SEC Financial Statement Data Sets

This is the repository for my final project.

## Presentation video

[![Presentation Video](https://img.youtube.com/vi/xl_xME5TFyE/0.jpg)](https://www.youtube.com/watch?v=xl_xME5TFyE&feature=youtu.be)

## Overview of submission materials

Included files:
  - [FinalPresentation.pdf](FinalPresentation.pdf): Slides used in presentation 
  - Code:
    - [DataDownload.R](DataDownload.R): Code used to download, unzip and re-sort the SEC data files into their component folders. 
	 - [TerminalCommands.txt](TerminalCommands.txt): Terminal commands used to move directories to the VM and cluster
	 - [FinalProjectScala.scala](FinalProjectScala.scala): Scala/Spark commands used to load and filter down the full data
	 - [FinalProjectR.r](FinalProjectR.r): R commands used to attempt to analyze the data.


## Useful Links:

- [Link to data sets](https://www.sec.gov/dera/data/financial-statement-data-sets.html): SEC Financial Statement Data Sets.
- [Description of SEC Data Set](https://www.sec.gov/files/aqfs.pdf): Documentation of SEC data set.
- [Dow Jones Industrial Average Members](https://en.wikipedia.org/wiki/Historical_components_of_the_Dow_Jones_Industrial_Average): History of Dow members.
- [SparkR Documentation](https://spark.apache.org/docs/1.6.0/api/R/): Documentation for SparkR that I used in the project.
- [quantmod Documentation](https://www.quantmod.com/documentation/00Index.html): Documentation for the quantmod R package. Used for pulling financial data.
- [xts Documentation](https://cran.r-project.org/web/packages/xts/xts.pdf): Documentation for the xts R package. Used for time series in R.
- [Microsoft Example Filing](https://www.sec.gov/Archives/edgar/data/789019/000156459019012709/msft-10q_20190331.htm): Example Microsoft earnings report from presentation.
- [Tag try order framework](http://www.xbrlsite.com/2014/Reference/Mapping.pdf): Mappings between accounting concepts and tags from 2014 that I consulted with.
