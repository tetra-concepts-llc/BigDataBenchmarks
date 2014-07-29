package hpda.spark.sort

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.examples.terasort.TeraInputFormat
import org.apache.hadoop.examples.terasort.TeraOutputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.InputFormatInfo
import scopt.OptionParser
import scala.math.random
import scala.collection.mutable._

object Terasort {

    val appName = "Spark Terasort"

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("Terasort") {
      opt[String]('u', "user") valueName ("user") action {
        (x, c) => c.copy(user = x)
      }

      arg[String]("master") valueName ("master") action {
        (x, c) => c.copy(master = x)
      }

      arg[String]("input") valueName ("input") action {
        (x, c) => c.copy(input = x)
      }

      arg[String]("output") valueName ("output") action {
        (x, c) => c.copy(output = x)
      }

      opt[Int]('s', "minSplits") valueName ("minSplists") action {
        (x, c) => c.copy(minSplits = x)
      }

      opt[String]('i', "dataFormat") valueName ("dataFormat") action {
        (x, c) => c.copy(dataFormat = x)
      }

    }

    parser.parse(args, Config()) map { config =>

      val format = if (config.dataFormat == "textFile") {
        classOf[org.apache.hadoop.mapred.TextInputFormat]
      } else {
        classOf[org.apache.hadoop.mapred.SequenceFileInputFormat[Text, Text]]
      }

      val hadoopConf = SparkHadoopUtil.get.newConfiguration()

      val myjar: Seq[String] = Seq(SparkContext.jarOfClass(this.getClass()) match {
        case Some(c) => c
        case None => throw new Exception("Could not locate Terasort jar for this operation")
      })

      val sc = new SparkContext(config.master, "Terasort", System.getenv("SPARK_HOME"),
        myjar,
        Map(),
        InputFormatInfo.computePreferredLocations(
          Seq(new InputFormatInfo(hadoopConf, format, config.input))))

      val sort = new Terasort(sc, config.input, config.output, config.minSplits)

      sc.stop()
      System.exit(0)
    } getOrElse {
      System.exit(1)
    }

  }

  @SerialVersionUID(1L)
  class Terasort(sc: SparkContext, input: String, output: String, minSplits: Int)
    extends Serializable {

    implicit val caseInsensitiveOrdering = new Ordering[Array[Byte]] {
        override def compare(a: Array[Byte], b: Array[Byte]) = a.compareTo(b)
    }

    val file = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](input, classOf[TeraInputFormat], classOf[Text], classOf[Text])
    val keyValue:RDD[(Array[Byte], Array[Byte])] = file.map(a => (a._1.getBytes(), a._2.getBytes()))
    val sortedValue = keyValue.sortByKey(true, 1)
    val backToText:RDD[(Text, Text)] = sortedValue.map{ case (k,v) => (new Text(k), new Text(v))}
    backToText.saveAsNewAPIHadoopFile(output, classOf[Text], classOf[Text], classOf[TeraOutputFormat])

    def getSortedOutputCount() = {
      backToText.count()
    }
  }

}