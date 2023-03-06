import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.rdd.RDD

object OddStuff {
  def run(args: Array[String]) {
    val opts = parseArgs(args)

    val spark = SparkSession.builder
      .master(opts.master)
      .appName("Odd stuff")
      .getOrCreate()

    // reading all files as text instead of csv because we can't trust all files to have the same format
    // e.g. some might be csv and some tsv
    // To my knowledge the csv data source only works with one type of seperator at a time
    // https://spark.apache.org/docs/latest/sql-data-sources-csv.html
    // This means custom parsing or preprocessing. This implementation chooses custom parsing.
    // Using RDD instead of dataset because we are reading the entire file no matter what, and
    // rather than convert between dataset and rdd and back, just stay in rdd
    val rdd: RDD[String] = spark.sparkContext.textFile(opts.inputDir)
    val res = process(rdd)

    opts.output match {
      case Some(outDir) => res.saveAsTextFile(outDir)
      case None         => res.foreach(println(_))
    }

    spark.stop()
  }

  def process(rdd: RDD[String]): RDD[String] = {
    rdd
      .mapPartitions(iter => {
        iter
          .map(parseRow(_))
          .filter(_.isDefined)
          .map(_.get)

      })
      .reduceByKey(merge)
      .map(pretty(_))
  }

  // Again, prefering correctness, and simplicity of parsing over performance.
  // This means rather than relying on dataframe machinery we are parsing rows ourselves.
  // This is necessary because delimiters my be either comma "," or tab "\t"
  // using a dataframe, all files in directory would need to have the same delimiter.

  // There is no guarantee that the rows aren't corrupted. There are two ways to deal with this.
  // 1. report corrupted rows, because solution doesn't make sense
  // 2. remove corrupted rows, because we want to find the closest approximation
  // I am taking the liberty of choosing the latter.
  def parseRow(row: String): Option[(Int, Set[Int])] = {
    val parts =
      if (row.contains(",")) row.split(",", -1)
      else row.split("\t", -1)

    val col1 = parts(0)
    val col2 = parts(1)

    for {
      k <- parseInt(col1)
      v <- parseInt(col2)
    } yield (k, Set(v))
  }

  // parsing a value can result in one of the following
  // 1. a header => invalid so thrown away
  // 2. whitespace => converted to 0
  // 3. a number => valid
  // 4. corrupt row => invalid so throw away
  def parseInt(s: String): Option[Int] = {
    val trimmed = s.trim
    try {
      Some(trimmed.toInt)
    } catch {
      case _: java.lang.NumberFormatException =>
        if (trimmed == "") Some(0)
        else None
    }
  }

  // Each set represents all the values that have an odd number of occurances
  // If a number is seen in both sets it means it was seen an odd number of times twice
  // odd + odd = even; so it can be removed
  // if a number has been seen an even number of times before it is irrelavant
  // odd + even = odd; so no need to keep track of even counts
  def merge(a: Set[Int], b: Set[Int]): Set[Int] = {
    (a diff b) union (b diff a) // all values that don't exist in both sets
  }

  // pretty gets value pairs ready to be written to file
  // if a row was corrupt then there is a possibility of multiple odds, reporting error in final result is simplist
  def pretty(pair: (Int, Set[Int])): String = {
    val values = pair._2
    val value =
      if (values.size == 1)
        values.head
      else if (values.isEmpty) {
        "Error: no odds found"
      } else {
        "Error: multiple odds found"
      }
    Array(pair._1, value).mkString("\t")
  }

  case class Opts(
      master: String = "local[*]",
      inputDir: String,
      output: Option[String] // If no output dir is given then print to stdout
  )

  // barbaric args parsing
  def parseArgs(args: Array[String]): Opts = {
    val output = if (args.length == 2) Some(args(1)) else None
    Opts(inputDir = args(0), output = output)
  }
}
