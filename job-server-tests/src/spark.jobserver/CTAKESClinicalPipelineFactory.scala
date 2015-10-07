package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.util.Try
import java.util.Properties

/**
 * <p>This class utilizes <a href="http://ctakes.apache.org">Apache cTAKES</a>
 * in order to run complete annotation pipelines for clinical documents
 * in plain text format using the built in UMLS (SNOMEDCT and RxNORM) dictionaries.
 * Basically, this tool extends the ClinicalPipelineFactory class by adding
 * the opportunity to run the cTAKES pipeline on arbitrary text instead
 * of using only the static text defined into <code>
 * ClinicalPipelineFactory.java</code>.</p>
 * <p>Set the config with the sentence/paragraph/text to split or count:
 * input.string = "adsfasdf asdkf  safksf a sdfa"
 *
 * validate() returns SparkJobInvalid if there is no input.string
 */
object CTAKESClinicalPipelineFactory extends SparkJob {
  
  def main(args: Array[String]): String = {
    val sc = new SparkContext("local[*]", "CTAKESClinicalPipeline")
    val inputconfig = ConfigFactory.parseString("")
    val config = ConfigFactory.load(inputconfig)
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val paraRDD = sc.parallelize(config.getString("input.string").split("\n").toSeq)
    println("Number of paragraphs detected in input data: " + paraRDD.count())
    paraRDD.map(new CtakesFunction())
    //paraRDD.map((_, 1)).reduceByKey(_ + _).collect().toMap
  }
}
