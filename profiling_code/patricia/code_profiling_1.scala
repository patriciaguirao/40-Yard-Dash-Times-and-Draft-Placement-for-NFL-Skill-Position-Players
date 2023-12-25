// Code profiling for numerical data

import scala.io.Source

// Load data -- file is in same folder
val filename = "NFL.csv"
val lines = Source.fromFile(filename).getLines().toSeq

// Extract the header row to get column titles
val header = lines.head.split(",").map(_.trim)

// Define the column titles that I want to process -- These are all the numerical columns
val columnsToProcess = Seq("Year", "Age", "Height", "Weight", "Sprint_40yd", "Vertical_Jump", "Bench_Press_Reps", "Broad_Jump", "Agility_3cone", "Shuttle", "BMI")

// Find the indices of the specified column titles
val columnIndices = columnsToProcess.map(title => header.indexOf(title))

// Function to parse a double (original data is String) or handle NA values
def parseDoubleWithNA(value: String): Option[Double] = {
  if (value == "NA") {
    None // Return None for NA values
  } else {
    Some(value.toDouble) // Parse double for valid values
  }
}

// Filter the data based on the selected columns and parse it
val data = lines.tail.map { line =>
  val values = line.split(",").map(_.trim)
  columnIndices.map(colIndex => parseDoubleWithNA(values(colIndex)))
}

// Transpose the data for later analysis
val transposedData = data.transpose

// Function to calculate mean
def calculateMean(column: Seq[Option[Double]]): Double = {
  val validValues = column.flatten // Remove None values
  if (!validValues.isEmpty) { // Use isEmpty to check for non-empty
    validValues.sum / validValues.length
  } else {
    0.0 // Handle cases where all values are NA
  }
}

// Function to calculate median
def calculateMedian(column: Seq[Option[Double]]): Double = {
  val validValues = column.flatten.sorted
  val n = validValues.length
  if (n % 2 == 0) {
    (validValues(n / 2 - 1) + validValues(n / 2)) / 2.0
  } else {
    validValues(n / 2)
  }
}

// Function to calculate mode
def calculateMode(column: Seq[Option[Double]]): Double = {
  val validValues = column.flatten
  val grouped = validValues.groupBy(identity).mapValues(_.length)
  val maxFrequency = grouped.values.max
  grouped.find { case (_, freq) => freq == maxFrequency }.map(_._1).getOrElse(0.0)
}

// Function to calculate standard deviation
def calculateStdDev(column: Seq[Option[Double]]): Double = {
  val validValues = column.flatten
  val mean = calculateMean(column)
  val squaredDifferences = validValues.map(x => Math.pow(x - mean, 2))
  Math.sqrt(squaredDifferences.sum / validValues.length)
}

// Calculate standard deviation for "Sprint_40yd"
val stdDevSprint40yd = calculateStdDev(transposedData(columnsToProcess.indexOf("Sprint_40yd")))

// Print the standard deviation result for "Sprint_40yd"
println(s"Standard Deviation for Sprint_40yd: $stdDevSprint40yd")

// Print the results for mean, median, mode with column titles
val means = transposedData.map(calculateMean)
val medians = transposedData.map(calculateMedian)
val modes = transposedData.map(calculateMode)

columnsToProcess.indices.foreach { idx =>
  println(s"Mean for Column ${columnsToProcess(idx)}: ${means(idx)}")
}
columnsToProcess.indices.foreach { idx =>
  println(s"Median for Column ${columnsToProcess(idx)}: ${medians(idx)}")
}
columnsToProcess.indices.foreach { idx =>
  println(s"Mode for Column ${columnsToProcess(idx)}: ${modes(idx)}")
}