import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class CassandraConnSettings(sparkContext: SparkContext) {

  val cassandraFormat = "org.apache.spark.sql.cassandra"
  val cassandraKeyspace = "template"
  val cassandraTable = "test"
  val cassandraOptions = Map("table" -> cassandraTable, "keyspace" -> cassandraKeyspace)
  val connector = CassandraConnector(sparkContext.getConf)
  executeCommand(connector, s"CREATE KEYSPACE IF NOT EXISTS $cassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  executeCommand(connector, s"CREATE TABLE IF NOT EXISTS $cassandraKeyspace.$cassandraTable (value varchar PRIMARY KEY)")
  val cassandraSchema = new StructType(Array(StructField("value", StringType, false)))

  private def executeCommand(conn: CassandraConnector, command: String): Unit = {
    conn.withSessionDo(session => session.execute(command))
  }

  def save(sqlContext: SQLContext, rdd: RDD[Row]): Unit = {
    sqlContext.createDataFrame(rdd, cassandraSchema)
      .write
      .format(cassandraFormat)
      .mode(Append)
      .options(cassandraOptions)
      .save()
  }
}

