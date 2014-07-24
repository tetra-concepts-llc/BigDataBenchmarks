package hpda.spark.sort

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.io.compress._ 
import org.apache.hadoop.io.Text
import scopt.OptionParser
import scala.math.random
import scala.collection.mutable._

/**
 * Sort in Spark
 * 
 * @author jmritz
 */

object sort {

    val appName = "Spark Sort"

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("Sort") {
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
        case None => throw new Exception("Could not locate Sort jar for this operation")
      })

      val sc = new SparkContext(config.master, "Sort", System.getenv("SPARK_HOME"),
        myjar,
        Map(),
        InputFormatInfo.computePreferredLocations(
          Seq(new InputFormatInfo(hadoopConf, format, config.input))))

      val sort = new Sort(sc, config.input, config.output, config.minSplits)

      sc.stop()
      System.exit(0)
    } getOrElse {
      System.exit(1)
    }

  }

  @SerialVersionUID(1L)
  class Sort(sc: SparkContext, input: String, output: String, minSplits: Int)
    extends Serializable {

    val file = sc.sequenceFile(input, classOf[Text], classOf[Text]).map{ case (k,v) => (k.toString(), v.toString())} 

    val sortedOutput = file.sortByKey(true, 1)
    
    sortedOutput.saveAsSequenceFile(output, Some(classOf[DefaultCodec]))

    def getSortedOutputCount() = {
      sortedOutput.count()
    }
  }


}