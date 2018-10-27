import scala.io.Source
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.collection.parallel.immutable.ParVector

object h1b_statistics {
  def main(args: Array[String]): Unit = {

    val pathVector = getVectorOfFiles(args(0) + "/input").map(_.getAbsolutePath())

    def processStats(in: ParVector[Option[ParVector[(String, Int)]]], totalCertified: Int) = {
      val removeOption = in.withFilter(_ != None).map(_.get)
      val grouped = removeOption.foldLeft(ParVector[(String, Int)]())(_ ++ _).groupBy(_._1)
      val reduced = grouped.mapValues(_.map(_._2).reduce(_ + _)).toVector
      val unsorted = reduced.map(in => (in._1, in._2, in._2.asInstanceOf[Double]*100/ totalCertified))
      unsorted.sortBy(_._1).sortBy(_._2)(Ordering[Int].reverse).take(10)
    }

    var stats: ParVector[(Option[ParVector[(String, Int)]], Option[ParVector[(String, Int)]])] =
      ParVector[(Option[ParVector[(String, Int)]], Option[ParVector[(String, Int)]])]()
    var totalCertified = 0
    for (path <- pathVector) {

      var inputStream = Source.fromFile(path).getLines.toStream

      def getFirstLine(): String = {
        val firstLine = inputStream.head
        inputStream = inputStream.tail
        firstLine
      }

      def getSomeLines(): ParVector[String] = {
        val someLines = inputStream.take(50000).toVector.par
        inputStream = inputStream.drop(50000)
        someLines
      }

      val indices: (Vector[Int], Vector[Int], Vector[Int]) = processHeader(getFirstLine)

      while (!inputStream.isEmpty && !indices._1.isEmpty) {
        val data = getSomeLines.map(replaceSemicolon(_)).map(_.split("""\^"""))

        val filteredData = data.filter(_(indices._1.head) == "CERTIFIED")

        val input: (ParVector[Array[String]], Vector[Int], Vector[Int]) = (filteredData, indices._2, indices._3)
        totalCertified = totalCertified + filteredData.length

        stats = computeStatistics(input) +: stats
      }
    }
    val occStats = processStats(stats.map(_._1), totalCertified)
    val staStats = processStats(stats.map(_._2), totalCertified)

    val occFile = new File(args(0) + "/output/top_10_occupations.txt")
    val occ_bw = new BufferedWriter(new FileWriter(occFile))
    occ_bw.write("TOP_OCCUPATIONS;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE")
    occ_bw.newLine
    for (i <- 0 until occStats.length) {
      val name = occStats(i)._1
      val count = occStats(i)._2
      val percent = occStats(i)._3
      occ_bw.write(f"$name;$count;$percent%.1f%%")
      occ_bw.newLine
    }
    occ_bw.close()

    val staFile = new File(args(0)+"/output/top_10_states.txt")
    val sta_bw = new BufferedWriter(new FileWriter(staFile))
    sta_bw.write("TOP_STATES;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE")
    sta_bw.newLine
    for (i <- 0 until staStats.length) {
      val name = staStats(i)._1
      val count = staStats(i)._2
      val percent = staStats(i)._3
      sta_bw.write(f"$name;$count;$percent%.1f%%")
      sta_bw.newLine
    }
    sta_bw.close()
  }

  def getVectorOfFiles(dir: String): Vector[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(in => in.isFile && in.getName != "README.md").toVector
    } else {
      Vector[File]()
    }
  }

  def replaceSemicolon(str:String):String = {
    val builder = StringBuilder.newBuilder
    var isInQuotes = false
    for(ch<-str){
      if (ch=='"') {
        isInQuotes = !isInQuotes
        builder.append(ch)
      }
      else if(ch==';' && !isInQuotes) builder.append('^')
      else builder.append(ch)
    }
    builder.toString
  }

  def processHeader(input: String): (Vector[Int], Vector[Int], Vector[Int]) = {
    val splitInput = input.split(";")

    def isMatch(in1: String, in2: Vector[String]): Boolean =
      if (in2.isEmpty) false else if (in1 == in2.head) true else isMatch(in1, in2.tail)

    def findText(line: Array[String], text: Vector[String], idx: Int): Vector[Int] =
      if (line.isEmpty) Vector[Int]()
      else if (isMatch(line.head, text)) Vector[Int](idx)
      else findText(line.tail, text, idx + 1)

    val occupationIdx = findText(splitInput, Vector("LCA_CASE_JOB_TITLE", "JOB_TITLE"), 0)
    val stateIdx = findText(splitInput, Vector("WORKSITE_STATE", "LCA_CASE_WORKLOC1_STATE"), 0) ++
      findText(splitInput, Vector("LCA_CASE_WORKLOC2_STATE"), 0) ++
      findText(splitInput, Vector("LCA_CASE_EMPLOYER_STATE", "EMPLOYER_STATE"), 0)
    val certifiedIdx = findText(splitInput, Vector("STATUS", "CASE_STATUS"), 0)
    (certifiedIdx, occupationIdx, stateIdx)
  }

  def computeStatistics(in: (ParVector[Array[String]], Vector[Int], Vector[Int])) = {
    def inner(data: ParVector[Array[String]], entryIdx: Vector[Int]): ParVector[(String, Int)] = {
      def notEmpty(entries: Array[String], entryIdx: Vector[Int]): Boolean =
        if (entryIdx.isEmpty) false else if (entries(entryIdx.head) != "") true else notEmpty(entries, entryIdx.tail)

      def getFirstNonEmpty(entries: Array[String], entryIdx: Vector[Int]): String =
        if (entries(entryIdx.head) != "") entries(entryIdx.head).trim else getFirstNonEmpty(entries, entryIdx.tail)

      val performMap = data.withFilter(in => notEmpty(in, entryIdx)).map(in => (getFirstNonEmpty(in, entryIdx), 1))
      performMap.groupBy(_._1).mapValues(_.map(_._2).reduce(_ + _)).toVector.par
    }

    val occupationStatistics =
      if (in._2 != None) Some(inner(in._1, in._2)) else None
    val stateStatistics = if (in._3 != None) Some(inner(in._1, in._3)) else None
    (occupationStatistics, stateStatistics)
  }
}
