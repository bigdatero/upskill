package geo

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.formatMapper.{GeoJsonReader, WktReader}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}

import scala.collection.JavaConverters._
import scala.io.Source


object GeoLearn {

  def main(): Unit = {

    val logger  = Logger.getLogger(this.getClass)
    logger.setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setAppName("GeoSparkRunnableExample")
    conf.setMaster("local[*]")
    // Enable GeoSpark custom Kryo serializer
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)

    val sc = new SparkContext(conf)

    val pointRDDInputLocation = "./resources/checkin.csv"
    val pointRDDOffset = 0
    val pointRDDSplitter = FileDataSplitter.CSV
    val carryOtherAttributes = true

    val objectRDD = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)

//    println(s"PointRDD count ${objectRDD.countWithoutDuplicates()}")
//    println(s"PointRDD\n ${objectRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

    val point = objectRDD.getRawSpatialRDD.top(1).asScala.head

    val polygonRDDInputLocation = "./resources/checkinshape.csv"
    val polygonRDDStartOffset = 0 // The coordinates start from Column 0
    val polygonRDDEndOffset = 9 // The coordinates end at Column 8
    val polygonRDDSplitter = FileDataSplitter.CSV
    val polygonRDD = new PolygonRDD(sc, polygonRDDInputLocation, polygonRDDStartOffset, polygonRDDEndOffset, polygonRDDSplitter, carryOtherAttributes)

//    println(s"polygonRDD count ${polygonRDD.countWithoutDuplicates()}")
//    println(s"polygonRDD\n ${polygonRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")


    val wktInputLocation = "./resources/WKT.csv"
    val wktColumn = 0
    val allowTopologyInvalidGeometries = true
    val skipSyntaxInvalidGeometries = false


//    println("WKT file\n" + Source.fromFile(wktInputLocation).getLines().mkString("\n"))
//    val wktRDD = WktReader.readToGeometryRDD(sc, wktInputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
//
//    println(s"wktRDD count ${wktRDD.countWithoutDuplicates()}")
//    println(s"wktRDD\n ${wktRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

//    val geoJsonInputLocation = "./resources/polygon.json"
//    val geoJsonlRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonInputLocation, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
//
//    println(s"geoJsonlRDD count ${geoJsonlRDD.countWithoutDuplicates()}")
//    println(s"geoJsonlRDD\n ${geoJsonlRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

    val shapefileInputLocation="./resources/shapefile"
    val shapefileRDD = ShapefileReader.readToGeometryRDD(sc, shapefileInputLocation)

    println(s"shapefileRDD count ${shapefileRDD.countWithoutDuplicates()}")
    println(s"shapefileRDD\n ${shapefileRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

  }
}

