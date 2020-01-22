import geo.GeoLearn
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class GeoLearnSpec extends FlatSpec with Matchers with BeforeAndAfterAll{

  val logger  = Logger.getLogger(this.getClass)
  logger.setLevel(Level.WARN)

  override def beforeAll() = {

  }

  "test" should "run" in {
    println("tests running")

    val geoLearn = GeoLearn
    geoLearn.main()
  }

}
