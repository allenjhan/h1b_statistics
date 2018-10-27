import scala.io.Source
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.collection.parallel.immutable.ParVector
import scala.math.round

object h1b_statistics {
  def main(args: Array[String]): Unit = {

    val pathVector = getVectorOfFiles(args(0) + "/input").map(_.getAbsolutePath())

    val (occStats, staStats) = handleInput(pathVector)

    handleOutput(args(0), occStats, staStats)
  }

  /** Examines all the files in the passed-in directory and creates a vector of File objects associated
    * with the files.
    *
    * @param dir path of the directory
    * @return a vector of
    */
  def getVectorOfFiles(dir: String): Vector[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(in => in.isFile && in.getName != "README.md").toVector
    } else {
      Vector[File]()
    }
  }

  /** This function takes a vector of paths and processes each of them in manageable chunks of 50,000 entries
    * so as not to overwhelm the JVM's memory limit.
    *
    * @param pathVector a vector of paths to input files
    * @return the statistics for top occupations and top states
    */
  def handleInput(pathVector:Vector[String]):(Vector[(String,Int,Double)],Vector[(String,Int,Double)]) = {
    var stats: ParVector[(Option[ParVector[(String, Int)]], Option[ParVector[(String, Int)]])] =
      ParVector[(Option[ParVector[(String, Int)]], Option[ParVector[(String, Int)]])]()
    var totalCertified = 0
    for (path <- pathVector) {

      var inputStream = Source.fromFile(path).getLines.toStream

      /** Returns the first line in the input and removes it from the input stream.
        *
        * @return the first line of the input stream
        */
      def getFirstLine(): String = {
        val firstLine = inputStream.head
        inputStream = inputStream.tail
        firstLine
      }

      /** Returns the first 50000 lines of the input and removes it from the input stream.
        *
        * @return the first 50000 lines of the input stream
        */
      def getSomeLines(): ParVector[String] = {
        val someLines = inputStream.take(50000).toVector.par
        inputStream = inputStream.drop(50000)
        someLines
      }

      val indices: (Vector[Int], Vector[Int], Vector[Int]) = processHeader(getFirstLine)

      while (!inputStream.isEmpty && !indices._1.isEmpty) {
        val data = getSomeLines.map(replaceSemicolon(_)).map(_.split("""\^"""))

        val filteredData = data.filter(_ (indices._1.head) == "CERTIFIED")

        val input: (ParVector[Array[String]], Vector[Int], Vector[Int]) = (filteredData, indices._2, indices._3)
        totalCertified = totalCertified + filteredData.length

        stats = computeStatistics(input) +: stats
      }
    }
    val occStats = processStats(stats.map(_._1), totalCertified)
    val staStats = processStats(stats.map(_._2), totalCertified)
    (occStats, staStats)
  }

  /** Because the input files have semicolons that appear within individual fields, naively splitting each line
    * by semicolon does not work. Instead, take all semicolons that don't appear in double quotes and replace them
    * with ^. That is what this function does: it replaces all semicolons that don't appear in quotes and replaces
    * them with ^ so that splitting the line on ^ works.
    *
    * Quotations appear in the input text as enclosed by one pair of double quotation marks, by one pair of two
    * double quotation marks to indicate quotation marks that appear within a field, or by one pair of three double
    * quotation marks. To handle all these cases, a simple rule that turns off semicolon replacement when a
    * double quotation mark is detected, and turns it back on when another double quotation mark is found. This always
    * works for single double quotes and triple double quotes, and happens to work for two double quotes because they
    * are always found within another quotation.
    *
    * All double quotation marks in the original string are removed in the final string
    *
    * @param str line of text to perform replace on
    * @return line of text with semicolons that don't appear in quotes replaced
    */
  def replaceSemicolon(str:String):String = {
    val builder = StringBuilder.newBuilder
    var isInQuotes = false
    for(ch<-str){
      if (ch=='"') isInQuotes = !isInQuotes
      else if(ch==';' && !isInQuotes) builder.append('^')
      else builder.append(ch)
    }
    builder.toString
  }

  /** This function processes the first line for each input file and parses it to determine where the relevant fields
    * we need to read occur. The indices of these fields are returned as vectors of integers because sometimes we need
    * to check more than one field for a given data point.
    *
    * @param input the first line of the text file, given as a string
    * @return the indices for status, occupation, and state, respectively
    */
  def processHeader(input: String): (Vector[Int], Vector[Int], Vector[Int]) = {
    val splitInput = input.split(";")

    /** Determines if a string matches any of the strings in a vector of strings
      *
      * @param in1 the string to check
      * @param in2 a vector of strings to check against
      * @return true if there is a match, false otherwise
      */
    def isMatch(in1: String, in2: Vector[String]): Boolean =
      if (in2.isEmpty) false else if (in1 == in2.head) true else isMatch(in1, in2.tail)

    /** Searches a line of text for the occurrence of any entries in a vector of strings
      *
      * @param line the line to search
      * @param text a vector of strings to check for
      * @param idx the index of the current entry being checked
      * @return the index where a string was found, enclosed in a vector; if not found, return empty vector
      */
    def findText(line: Array[String], text: Vector[String], idx: Int): Vector[Int] =
      if (line.isEmpty) Vector[Int]()
      else if (isMatch(line.head, text)) Vector[Int](idx)
      else findText(line.tail, text, idx + 1)

    val occupationIdx = findText(splitInput, Vector("LCA_CASE_SOC_NAME", "SOC_NAME"), 0)
    val stateIdx = findText(splitInput, Vector("WORKSITE_STATE", "LCA_CASE_WORKLOC1_STATE"), 0) ++
      findText(splitInput, Vector("LCA_CASE_WORKLOC2_STATE"), 0) ++
      findText(splitInput, Vector("LCA_CASE_EMPLOYER_STATE", "EMPLOYER_STATE"), 0)
    val certifiedIdx = findText(splitInput, Vector("STATUS", "CASE_STATUS"), 0)
    (certifiedIdx, occupationIdx, stateIdx)
  }

  /** This function takes the raw data that has been split on ^ and outputs the occupation statistics and state
    * statistics.
    *
    * @param in The first field is the raw data. Each split line is given as an array. The second field is the sequence
    *           of indices to check for occupation. The third field is the sequence of indices to check for state.
    * @return The return value is a tuple containing the occupation statistics and state statistics. Each tuple of
    *         statistics consists of a key, which is a String, and the number of occurrences of that key in the data,
    *         which is an Int.
    */
  def computeStatistics(in: (ParVector[Array[String]], Vector[Int], Vector[Int])) = {

    /** The inner function for the computeStatistics function. Computes the statistics for the current batch of data.
      *
      * @param data the data passed as a collection of split text
      * @param entryIdx the indices at which to search for information
      * @return the statistics for the entry to which the index corresponds
      */
    def inner(data: ParVector[Array[String]], entryIdx: Vector[Int]): ParVector[(String, Int)] = {

      /** Returns true if at least one entry corresponding to the given indices is not an empty string.
        *
        * @param entries the line of data to parse
        * @param entryIdx the indices at which to check
        * @return true if at least one field is not empty string, and false otherwise
        */
      def notEmpty(entries: Array[String], entryIdx: Vector[Int]): Boolean =
        if (entryIdx.isEmpty) false else if (entries(entryIdx.head) != "") true else notEmpty(entries, entryIdx.tail)

      /** Get the first entry that is not an empty string.
        *
        * @param entries the line of data to parse
        * @param entryIdx the indices at which to extract data
        * @return the found string at the first non-empty entry
        */
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

  /** After the statistics have been computed for each batch, they have to be combined in a reduce operation that
    * sums up the number of occurrences from each batch. An extra field is computed that includes the percentage of
    * the total entries in which a key appears.
    *
    * @param in a sequence of keys and the number of occurrences for that key
    * @param totalCertified the total number of certified entries; used to calculate percentage of occurrences for
    *                       each key
    * @return a vector of tuples, where the tuple consists of the keys, their number of occurrences, and the
    *         percentage of occurrences for each key
    */
  def processStats(in: ParVector[Option[ParVector[(String, Int)]]], totalCertified: Int) = {
    val removeOption = in.withFilter(_ != None).map(_.get)
    val grouped = removeOption.foldLeft(ParVector[(String, Int)]())(_ ++ _).groupBy(_._1)
    val reduced = grouped.mapValues(_.map(_._2).reduce(_ + _)).toVector
    val unsorted = reduced.map(in => (in._1, in._2, round(in._2.asInstanceOf[Double]*1000/ totalCertified)/10.0))
    unsorted.sortBy(_._1).sortBy(_._2)(Ordering[Int].reverse).take(10)
  }

  /** Outputs the top ten entries for occupations and states into files in the output folder.
    *
    * @param dir the top level directory of the project; used to generate the paths for each output file
    * @param occStats the occupation statistics
    * @param staStats the state statistics
    */
  def handleOutput(dir:String, occStats:Vector[(String,Int,Double)], staStats:Vector[(String,Int,Double)]) = {
    val occFile = new File(dir + "/output/top_10_occupations.txt")
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

    val staFile = new File(dir + "/output/top_10_states.txt")
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
}
