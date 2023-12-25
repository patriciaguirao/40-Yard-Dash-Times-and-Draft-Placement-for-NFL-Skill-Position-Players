// Code profiling for non-numerical data

import scala.io.Source

// Load data -- file is in the same folder
val filename = "NFL.csv"
val lines = Source.fromFile(filename).getLines().toSeq

// Extract the header row to get column titles
val header = lines.head.split(",").map(_.trim)

// Find the index of the column to parse
val columnIndexToParse = header.indexOf("Drafted..tm.rnd.yr.")

// Text Formatting
// Cleaning this column that is originally structured as "Arizona Cardinals / 1st / 31st pick / 2009" to be:
// - separate the values by "/"
// - drop the year at the end because that's already its own column
// - turn the numerical values (ex. "1st" and "31st pick") to be Integer values instead of Strings

// Define the new column titles after parsing
val newColumnTitles = Seq("Team", "Round", "Overall_Pick")

// Find the indices of the other columns to keep
val columnsToKeepIndices = Seq(header.indexOf("School"), header.indexOf("Player_Type"), header.indexOf("Position_Type"), header.indexOf("Position"), header.indexOf("Drafted"))

// Function to parse a value and return the new column -- Just get team name
def parseAndSplit(value: String): Seq[String] = {
  if (value == "NA") {
    Seq("Undrafted") // Set "Undrafted" for NA values
  } else {
    val parsedColumns = value.split("/").map(_.trim)
    parsedColumns.dropRight(3).map { col => col }
  }
}

// Process and rewrite the data
val newData = lines.tail.map { line =>
  val values = line.split(",").map(_.trim)
  val parsedColumns = parseAndSplit(values(columnIndexToParse))
  val newLine = columnsToKeepIndices.map(index => values(index)) ++ parsedColumns
  newLine.mkString(",")
}

val columnTitles = "School,Player_Type,Position_Type,Position,Drafted,Team"

// Save the new file to HDFS in a folder titled final -- Make it 1 file
val hdfsDestination = "hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/NFLProfiling"
val newDataRDD = sc.parallelize(columnTitles +: newData)  // Include new titles as the first line
newDataRDD.coalesce(1).saveAsTextFile(hdfsDestination)