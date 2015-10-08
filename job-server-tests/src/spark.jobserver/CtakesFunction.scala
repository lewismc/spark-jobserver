package spark.jobserver

import java.io.ByteArrayOutputStream

import org.apache.uima.UIMAException
import org.apache.uima.jcas.JCas
import org.apache.uima.analysis_engine.AnalysisEngineDescription
import org.apache.uima.cas.impl.XmiCasSerializer
import org.apache.uima.fit.factory.JCasFactory
import org.apache.uima.fit.pipeline.SimplePipeline
import org.apache.uima.util.XMLSerializer
import org.apache.spark.api.java.function.Function

import it.cnr.iac.CTAKESClinicalPipelineFactory

object CtakesFunction extends Function[String, String] {
  override def call(para: String): String = {
    println("Creating UIMA JCas object.")
    val jcas = JCasFactory.createJCas()
    jcas.setDocumentText(para)
    val pipeline = CTAKESClinicalPipelineFactory.getDefaultPipeline()
    println("Completed instantiation of cTAKES default clinical pipeline")
    // final AnalysisEngineDescription aed = getFastPipeline(); // Outputs
    // from default and fast pipelines are identical
    val baos = new ByteArrayOutputStream()
    SimplePipeline.runPipeline(jcas, pipeline)
    val xmiSerializer = new XmiCasSerializer(jcas.getTypeSystem())
    val xmlSerializer = new XMLSerializer(baos, true)
    xmiSerializer.serialize(jcas.getCas(), xmlSerializer.getContentHandler())
    jcas.reset()
    baos.toString("utf-8")
  }
}