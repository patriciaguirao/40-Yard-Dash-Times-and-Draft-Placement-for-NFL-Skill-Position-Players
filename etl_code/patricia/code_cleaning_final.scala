import scala.io.Source

// Load data -- file is in the same folder
val filename = "NFL.csv"
val lines = Source.fromFile(filename).getLines().toSeq

// Extract the header row to get column titles
val header = lines.head.split(",").map(_.trim)

// Find the indexes of the columns to parse
val columnIndexToParse1 = header.indexOf("Player")
val columnIndexToParse2 = header.indexOf("Drafted..tm.rnd.yr.")

// Find the indices of the other columns to keep
val columnsToKeepIndices = Seq(header.indexOf("Position"), header.indexOf("Sprint_40yd"))

// Function to parse a value and return the new column -- Changing Player column to just keep the actual names of players because many columns look like this "Beanie Wells\WellCh00"
def parseAndSplit1(value: String): Seq[String] = {
  if (value.contains("\\")) {
    val parsedColumns = value.split("\\\\").map(_.trim)
    parsedColumns.dropRight(1).map(col => col)
  } else {
    Seq(value)
  }
}

// Function to parse a value and return the new column -- Getting just round value from column that looks like this "Arizona Cardinals / 1st / 31st pick / 2009"
def parseAndSplit2(value: String): Seq[String] = {
  if (value == "NA") {
    Seq("Undrafted") // Set "Undrafted" for NA values
  } else {
    val parsedColumns = value.split("/").map(_.trim)
    parsedColumns.drop(1).dropRight(2).map { col =>
      if (col.exists(_.isDigit)) {
        col.filter(_.isDigit).toInt.toString
      } else {
        col
      }
    }
  }
}

// Process and rewrite the data, only include rows with QB, WR, TE, or RB in the "Position" column
val newData = lines.tail.filter { line =>
  val values = line.split(",").map(_.trim)
  val position = values(header.indexOf("Position"))
  position == "QB" || position == "WR" || position == "TE" || position == "RB"
}.map { line =>
  val values = line.split(",").map(_.trim)
  val parsedColumnPlayer = parseAndSplit1(values(columnIndexToParse1))
  val parsedColumnRound = parseAndSplit2(values(columnIndexToParse2))
  val newLine = parsedColumnPlayer ++ columnsToKeepIndices.map(values) ++ parsedColumnRound
  newLine.mkString(",")
}

// Only want to keep these columns and get rid of the rest
val columnTitles = "Player,Pos,40yd,Round"

// Save the new file to HDFS in a folder titled final -- Make it 1 file
val hdfsDestination = "hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/NFLCleanFinal"
val newDataRDD = sc.parallelize(columnTitles +: newData)  // Include new titles as the first line
newDataRDD.coalesce(1).saveAsTextFile(hdfsDestination)