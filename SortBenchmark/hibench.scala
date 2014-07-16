package hpda.spark.sort

/**
 * @author jmritz
 * 
 * The purpose of this Scala class is to recreate the HiBench Sort class written in map/reduce utilizing Apache Spark.  
 * The output from running the main class will be compared to the output from the HiBench Sort program.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.io.compress._ 

//-----------------------------------------------------------------------
//This is the only object of the sort package.  Its purpose is to house -
//the Scala code that will generate the sorted output                   -
//-----------------------------------------------------------------------
object hibench
{
  
//-----------------------------------------------------------------------
//The following vals are used to create a Spark instance.  All of those -
//are required to create a Spark Conf.                                  -
//-----------------------------------------------------------------------
  val appName = "SortHDFSSequenceFile";
  val master = "local";
  val conf = new SparkConf().setAppName(appName).setMaster(master);
  val sc = new SparkContext(conf);
  
//-----------------------------------------------------------------------
//The following vals are used to define input and output paths for the  -
//hdfs files we are reading and writing.                                -
//-----------------------------------------------------------------------
  //val secPath = "hdfs://localhost:9000/in/testInputSeq"
  val secPath = "hdfs://localhost:9000/in/hiBenchFiltered"
  val secPathOut = "hdfs://localhost:9000/out/hiBenchGen_spark"
  val saveOutPath = "hdfs://localhost:9000/out/sparkSortResultSeq"
  val saveInPath = "hdfs://localhost:9000/in/testInput3Seq"
//-----------------------------------------------------------------------
//The following val is used to define a date format used for logging.   -
//-----------------------------------------------------------------------
  val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  
//-----------------------------------------------------------------------
//The following method is used generate a log output to the screen.  The-
//method takes in a String named txtOutput.  The txtOutput variable can -
//contain any string define in the method call.  What ever value is sent-
//will be displayed before the current date/time.                       -
//-----------------------------------------------------------------------
  def log(txtOutput: String)
  {
    var dtTime = new Date()
    println(txtOutput + dateFormat.format(dtTime))
    
  }

//-----------------------------------------------------------------------
//The following method is the main method.  This method controls the    -
//flow of the Scala object.  Its purpose in this class is to read in a  -
//HDFS sequence file and sort the file based on the key.  The sorting   -
//will be executed in descending order to match the HiBench results.    -
//Finally, the results will be stored in a sequence file.               -
//-----------------------------------------------------------------------
  def main(args : Array[String])
  {
      //Read in the HDFS sequence input file.  Please note that the map 
      //IS NECESSARY for this method to complete successfully.  Without
      //it, it will try to serialize the Text input/output class.  This 
      //class is not serializable.  
	  log("Start reading Hadoop Sequence file. ")	  
	  val rdd:RDD[(String, String)] = sc.sequenceFile(secPath,
	    						    classOf[Text],
	    						    classOf[Text]
	    						   ).map { case (k,v) => (k.toString, v.toString)}
	  log("Stop reading Hadoop Sequence file. ")

	  //Sort the HDFS sequence input file.  This step will sort the data
	  //by the key in descending order.  It also defines the result will
	  //contain 1 partition.  This is set at one because of the current
	  //hardware platform.  
	  
	  log("Start sort by key. ")
	  val sortedOutput = rdd.sortByKey(true, 1)
	  log("Stop sort by key. ")
	  
	  //Save the sorted result as a sequence file.  
	  log("Start output sequence file save. ")
	  sortedOutput.saveAsSequenceFile(secPathOut, Some(classOf[DefaultCodec]))					   
	  log("Stop output sequence file save. ")
	  
	  //Output records counts to verify the number of input records 
	  //match the number of output records.  I realized that sorting
	  //data should not result in data loss
	  println("Number of input records: " + rdd.count)
	  println("Number of sorted records: " + sortedOutput.count)
	  
  }

}