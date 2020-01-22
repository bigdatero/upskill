package geo

import java.io.File

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.geosparksql.expressions.ST_Point
import org.apache.spark.sql.types.DataType
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.formatMapper.{GeoJsonReader, WktReader}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.Adapter

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
    val sparkSession = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
//      .enableHiveSupport()
      .getOrCreate()

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
    val wktRDD = WktReader.readToGeometryRDD(sc, wktInputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
//
//    println(s"wktRDD count ${wktRDD.countWithoutDuplicates()}")
//    println(s"wktRDD\n ${wktRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

    val geoJsonInputLocation = "./resources/polygon.json"
    val geoJsonlRDD = GeoJsonReader.readToGeometryRDD(sc, geoJsonInputLocation, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
//
//    println(s"geoJsonlRDD count ${geoJsonlRDD.countWithoutDuplicates()}")
//    println(s"geoJsonlRDD\n ${geoJsonlRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

    val shapefileInputLocation="./resources/shapefile"
    val shapefileRDD = ShapefileReader.readToGeometryRDD(sc, shapefileInputLocation)

//    println(s"shapefileRDD count ${shapefileRDD.countWithoutDuplicates()}")
//    println(s"shapefileRDD\n ${shapefileRDD.getRawSpatialRDD.collect().asScala.mkString("\n")}")

    val sqlDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(pointRDDInputLocation)
    sqlDf.createOrReplaceTempView("inputtable")

    FunctionRegistry.builtin.registerFunction("ST_Point", ST_Point)
    sparkSession.udf.register("ST_Point", ST_Point)

//    println(s"sqlDf\n")
//    sqlDf.show()
//
//    println(s"schema\n")
//    sqlDf.printSchema()

    val castDf = sqlDf
      .withColumn("_c0",sqlDf.col("_c0").cast("double"))
      .withColumn("_c1",sqlDf.col("_c1").cast("double"))
    sqlDf.createOrReplaceTempView("castinputtable")

//    println(s"castDf\n")
//    castDf.show()
//
//    println(s"castDfschema\n")
//    castDf.printSchema()

//    val spatialSqlDf = sparkSession.sql(
//      """
//        |SELECT ST_Point(CAST(castinputtable._c0 AS Decimal(24,20)),CAST(castinputtable._c1 AS Decimal(24,20))) AS checkin
//        |FROM castinputtable
//    """.stripMargin)
//
////    val spatialSqlDf = sparkSession.sql(
////      """
////        |SELECT ST_Point(castinputtable._c0,castinputtable._c1) AS checkin
////        |FROM castinputtable
////    """.stripMargin)

//    val sqlSpatialRDD = new SpatialRDD[Geometry]
//    sqlSpatialRDD.rawSpatialRDD = Adapter.toRdd(spatialSqlDf)
//
//    println(s"spatialSqlDf\n")
//    spatialSqlDf.show()

//    println(s"shapefileRDD\n ${spatialSqlDf.rawSpatialRDD.collect().asScala.mkString("\n")}")

    val pointRDD = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)
    val changeRefSysRDD = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)
    changeRefSysRDD.CRSTransform("epsg:4326", "epsg:3857")

//    println(s"pointRDD\n${pointRDD.rawSpatialRDD.collect().asScala.mkString("\n")}")
//    println(s"changeRefSysRDD\n${changeRefSysRDD.rawSpatialRDD.collect().asScala.mkString("\n")}")

    val rddWithOtherAttributes = pointRDD.rawSpatialRDD.rdd.map(point => point.getUserData.asInstanceOf[String])

//    println("rddWithOtherAttributes\n" + rddWithOtherAttributes.collect().mkString("\n"))

    val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
    //    Defines a rectangular region of the 2D coordinate plane.
    //    It is often used to represent the bounding box of a Geometry,
    //    e.g. the minimum and maximum x and y values of the Coordinates.

    val considerBoundaryIntersection = false // Only return gemeotries fully covered by the window
    val usingIndex = false
    var queryResult = RangeQuery.SpatialRangeQuery(polygonRDD, rangeQueryWindow, considerBoundaryIntersection, usingIndex)

//    println(s"queryResult\n${queryResult.collect().asScala.mkString("\n")}")

    val geometryFactory = new GeometryFactory()
    val coordinates = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(0,0)
    coordinates(1) = new Coordinate(0,4)
    coordinates(2) = new Coordinate(4,4)
    coordinates(3) = new Coordinate(4,0)
    coordinates(4) = coordinates(0)
    val polygonObject = geometryFactory.createPolygon(coordinates)

    val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
    polygonRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

    val usingIndexTrue = true
    var indexQueryResult = RangeQuery.SpatialRangeQuery(polygonRDD, rangeQueryWindow, considerBoundaryIntersection, usingIndexTrue)

//    println(s"indexQueryResult\n${indexQueryResult.collect().asScala.mkString("\n")}")

//    val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
//    val K = 1000 // K Nearest Neighbors
//    val KNNQueryResult = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)

//    println(KNNQueryResult)

    objectRDD.analyze()

    //Two SpatialRDD must be partitioned by the same way.
    objectRDD.spatialPartitioning(GridType.KDBTREE)
    pointRDD.spatialPartitioning(objectRDD.getPartitioner)

//    Each object on the left is covered/intersected by the object on the right.

    val spatialJoinQuery = JoinQuery.SpatialJoinQuery(objectRDD, pointRDD, usingIndex, considerBoundaryIntersection)

//    println("spatialJoinQuery \n"+ spatialJoinQuery.collect().asScala.mkString("\n"))

    val indexSpatialJoinQuery = JoinQuery.SpatialJoinQueryFlat(objectRDD, pointRDD, usingIndexTrue, considerBoundaryIntersection)

    val circleRDD = new CircleRDD(pointRDD, 0.1) // Create a CircleRDD using the given distance

    println("circleRDD \n"+ circleRDD.getCenterPointAsSpatialRDD.rawSpatialRDD.collect().asScala.mkString("\n"))

    circleRDD.analyze()
    circleRDD.spatialPartitioning(GridType.KDBTREE)
    polygonRDD.spatialPartitioning(circleRDD.getPartitioner)

    val distanceJoinQueryFlat = JoinQuery.DistanceJoinQueryFlat(polygonRDD, circleRDD, usingIndex, considerBoundaryIntersection)


  }
}

