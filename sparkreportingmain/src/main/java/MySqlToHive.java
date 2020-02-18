import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.first;

public class MySqlToHive {
    public static void main(String[] args) {

        SparkSession hiveSession = SparkSession
                .builder()
                .appName("interfacing spark sql to hive metastore without configuration file")
                .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
                .master("local[*]")
                .enableHiveSupport() // don't forget to enable hive support
                .getOrCreate();


        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkFileSumApp")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.memory", "8g")
                .master("local[*]")
                .getOrCreate();
        String mysqlUrl = "jdbc:mysql://localhost/mystuff?createDatabaseIfNotExist=true";
        String query = "(select * from mydata) mydata";
        Dataset<Row> dataframe = sparkSession
                .read()
                .format("jdbc")
                .option("url", mysqlUrl)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable",query)
                .option("user","mystuff")
                .option("password","mystuff123")
                .load();
        dataframe.persist();
        Dataset<Row> resultSet = dataframe
                .groupBy("id")
                .pivot("k")
                .agg(first("v"));
        resultSet.persist();
        Dataset<Row> df = resultSet.toDF();
        System.out.println("WRITING HIVE");
        hiveSession.createDataFrame(df.rdd(), df.schema())
                .write()
                .mode(SaveMode.Append)
                .saveAsTable("person");
        System.out.println("Number of rows are :" + resultSet.count());
        sparkSession.close();
    }
}