import scala.io.Source
import java.io.PrintWriter

val input = "allcombinedata.csv"
val output = "cleanedCombineData.csv"
val lines = Source.fromFile(input).getLines().toList
val header = lines.head.split(",").map(_.trim)
val playerIndex = header.indexOf("Player")
val posIndex = header.indexOf("Pos")
val fortyYdIndex = header.indexOf("40yd")
val draftedIndex = header.indexOf("Drafted (tm/rnd/yr)")

//Positions to keep
val positionsToKeep = Set("QB", "WR", "TE", "RB")
val writer = new PrintWriter(output)
// Parse the Drafted column and extract the Round Drafted or Undrafted
def parseDraftedColumn(value: String): String = {
  if (value.isEmpty) "Undrafted" else value.split("/")(1).trim.filter(_.isDigit)
}

writer.println(s"${header(playerIndex)},${header(posIndex)},${header(fortyYdIndex)},Round")

lines.tail.foreach { line =>
  val columns = line.split(",", -1).map(_.trim)
  if (columns.length > draftedIndex && positionsToKeep.contains(columns(posIndex))) {
    val fortyYd = columns(fortyYdIndex)
    if (fortyYd.nonEmpty && fortyYd != "N/A") {
      val round = parseDraftedColumn(columns(draftedIndex))
      writer.println(s"${columns(playerIndex)},${columns(posIndex)},$fortyYd,$round")
    }
  }
}

writer.close()

