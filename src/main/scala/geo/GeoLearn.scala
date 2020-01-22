package geo

import com.vividsolutions.jts.io.{WKTFileReader, WKTReader}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.geosparksql.expressions.ST_GeomFromWKT
import org.apache.spark.storage.StorageLevel
import org.geotools.geometry.jts.WKTReader2

import collection.JavaConverters._


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

    println(s"PointRDD count ${objectRDD.countWithoutDuplicates()}")
    println(s"PointRDD\n ${objectRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

    val point = objectRDD.getRawSpatialRDD.top(1).asScala.head

    val polygonRDDInputLocation = "./resources/checkinshape.csv"
    val polygonRDDStartOffset = 0 // The coordinates start from Column 0
    val polygonRDDEndOffset = 9 // The coordinates end at Column 8
    val polygonRDDSplitter = FileDataSplitter.CSV
    val polygonRDD = new PolygonRDD(sc, polygonRDDInputLocation, polygonRDDStartOffset, polygonRDDEndOffset, polygonRDDSplitter, carryOtherAttributes)

    println(s"polygonRDD count ${polygonRDD.countWithoutDuplicates()}")
    println(s"polygonRDD\n ${polygonRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")


//    val wktInputLocation = "./resources/WKT.csv"
//    val wktColumn = 0
//    val allowTopologyInvalidGeometries = true
//    val skipSyntaxInvalidGeometries = false
//
//    val spatialRDD = new PolygonRDD(sc, wktInputLocation, FileDataSplitter.WKT, true, StorageLevel.MEMORY_ONLY)


//    public void testWktConstructor()
//    {
//      PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocationWkt, FileDataSplitter.WKT, true, StorageLevel.MEMORY_ONLY());
//      assert spatialRDD.approximateTotalCount == 103;
//      assert spatialRDD.boundaryEnvelope != null;
//      assert spatialRDD.rawSpatialRDD.take(1).get(0).getUserData().equals("31\t039\t00835841\t31039\tCuming\tCuming County\t06\tH1\tG4020\t\t\t\tA\t1477895811\t10447360\t+41.9158651\t-096.7885168");
//    }


//    val wktRDD2 = new WKTReader2 //.readToGeometryRDD(sparkSession.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
//    wktRDD2.read("")


//    println(s"wktRDD count ${wktRDD.count()}")
//    println(s"wktRDD\n ${wktRDD.collect().asScala.mkString("\n")}")




  }
}

