import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveConfigContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog;
import org.apache.spark.sql.hive.HiveContext;
import scala.Function1;
import scala.math.Ordering;


import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {

//        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
  //      conf.set("spark.driver.memory", "8g");
    //    JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession hiveSession = SparkSession
                .builder()
                .appName("interfacing spark sql to hive metastore without configuration file")
                .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
                .master("local[*]")
                .enableHiveSupport() // don't forget to enable hive support
                .getOrCreate();
        hiveSession.sql("create table if not exists person (name string, email string) row format delimited fields terminated by ',' stored as textfile");


        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkFileSumApp")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.memory", "8g")
                .master("local[*]")
                .getOrCreate();
      //  SQLContext sqlContext = new SQLContext(sc);
        String minMaxQuery = "(select min(source_time_created), max(source_time_created) from partner_transaction) emp";
        Dataset<Row> minMaxDataframe = sparkSession
                .read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:HAWAAPP/HAWAPPSTG@msmaster-db-rw-a.qa.paypal.com:2126/SRV_MSM")
                .option("dbtable",minMaxQuery)
                .option("user","HAWAAPP")
                .option("password","HAWAAPPSTG")
                .option("customSchema", "SOURCE_TIME_CREATED decimal(38,0), CUSTOMER_ACTIVITY_KEY decimal(38,0), TRANSACTION_ACTION_ID decimal(38,0), TRANSACTION_ID decimal(38,0), PARENT_TRANSACTION_ID decimal(38,0), PARTNER_ACCOUNT_NUMBER decimal(38,0), MOD_PARTNER_ACCOUNT_NUMBER decimal(38,0), MERCHANT_ACCOUNT_NUMBER decimal(38,0), GROSS_AMOUNT decimal(28,10), FEE_AMOUNT decimal(28,10), COUNTER_ACCOUNT_NUMBER decimal(38,0), TRANSACTION_TIME_CREATED decimal(38,0), TRANSACTION_TIME_UPDATED decimal(38,0), FLAGS_01 decimal(38,0), FLAGS_02 decimal(38,0), FLAGS_03 decimal(38,0), FLAGS_04 decimal(38,0), FLAGS_05 decimal(38,0), FLAGS_06 decimal(38,0), FLAGS_07 decimal(38,0), SHARED_ID decimal(38,0), USD_MONEY_AMOUNT decimal(28,10), TRANSACTION_PAYMENT_ID decimal(38,0), PAYPAL_FIXED_FEE_AMOUNT decimal(28,10), PAYPAL_VAR_FEE_AMOUNT decimal(28,10), DISBURSEMENT_TIME decimal(38,0), DISBURSEMENT_AMOUNT decimal(28,10), TIME_CREATED decimal(38,0), TIME_UPDATED decimal(38,0), BASE_FEE_PERCENTAGE decimal(28,10), ACTUAL_FEE_PERCENTAGE decimal(28,10), BASE_VAR_FEE_MONEY_AMOUNT decimal(28,10), BASE_FIXED_FEE_MONEY_AMOUNT decimal(28,10)")
                .load();
        long low = minMaxDataframe.head().getDecimal(0).longValue();
        long high = minMaxDataframe.head().getDecimal(1).longValue();
        String query = "(select * from partner_transaction where partner_identity = 'PAYPAL.FACEBOOKTEST' limit 20000) emp";
//        Dataset<Row> dataframe = sqlContext
        String[] transactionTypeColumns = {"TRANSACTION_TYPE_CODE", "TRANSACTION_SUBTYPE_CODE", "TRANSACTION_REASON_CODE"};
        Dataset<Row> dataframeRaw = sparkSession
                .read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:HAWAAPP/HAWAPPSTG@msmaster-db-rw-a.qa.paypal.com:2126/SRV_MSM")
                .option("dbtable","partner_transaction")
                .option("user","HAWAAPP")
                .option("password","HAWAAPPSTG")
                .option("partitionColumn", "source_time_created")
                .option("lowerBound", low)
                .option("upperBound", high)
                .option("numPartitions", 300)
                .option("customSchema", "SOURCE_TIME_CREATED decimal(38,0), CUSTOMER_ACTIVITY_KEY decimal(38,0), TRANSACTION_ACTION_ID decimal(38,0), TRANSACTION_ID decimal(38,0), PARENT_TRANSACTION_ID decimal(38,0), PARTNER_ACCOUNT_NUMBER decimal(38,0), MOD_PARTNER_ACCOUNT_NUMBER decimal(38,0), MERCHANT_ACCOUNT_NUMBER decimal(38,0), GROSS_AMOUNT decimal(28,10), FEE_AMOUNT decimal(28,10), COUNTER_ACCOUNT_NUMBER decimal(38,0), TRANSACTION_TIME_CREATED decimal(38,0), TRANSACTION_TIME_UPDATED decimal(38,0), FLAGS_01 decimal(38,0), FLAGS_02 decimal(38,0), FLAGS_03 decimal(38,0), FLAGS_04 decimal(38,0), FLAGS_05 decimal(38,0), FLAGS_06 decimal(38,0), FLAGS_07 decimal(38,0), SHARED_ID decimal(38,0), USD_MONEY_AMOUNT decimal(28,10), TRANSACTION_PAYMENT_ID decimal(38,0), PAYPAL_FIXED_FEE_AMOUNT decimal(28,10), PAYPAL_VAR_FEE_AMOUNT decimal(28,10), DISBURSEMENT_TIME decimal(38,0), DISBURSEMENT_AMOUNT decimal(28,10), TIME_CREATED decimal(38,0), TIME_UPDATED decimal(38,0), BASE_FEE_PERCENTAGE decimal(28,10), ACTUAL_FEE_PERCENTAGE decimal(28,10), BASE_VAR_FEE_MONEY_AMOUNT decimal(28,10), BASE_FIXED_FEE_MONEY_AMOUNT decimal(28,10)")
                .load().na().fill("", transactionTypeColumns);
        Dataset<Row> dataframe = dataframeRaw.withColumn("pivot_code",
                concat(dataframeRaw.col("TRANSACTION_TYPE_CODE"),
                        dataframeRaw.col("TRANSACTION_SUBTYPE_CODE"),
                        dataframeRaw.col("TRANSACTION_REASON_CODE")));
        dataframe.persist();
        String typeQuery = "(select PIVOT_CODE, transaction_type_code, transaction_subtype_code,TRANSACTION_REASON_CODE from partner_transaction group by transaction_type_code, transaction_subtype_code, TRANSACTION_REASON_CODE) txn";
        Dataset<Row> txnDataframe = dataframe.groupBy("PIVOT_CODE").agg( first("PIVOT_CODE"));
        txnDataframe.show();
        Set<String> pivotCodeList = new HashSet<>();
        for (Row r : txnDataframe.collectAsList()) {
            String val = r.getAs("PIVOT_CODE");
            pivotCodeList.add(val);
        }

        Dataset<Row> resultSet = dataframe
                .groupBy("transaction_payment_id", "source_time_created")
                .pivot("PIVOT_CODE", pivotCodeList.stream().collect(Collectors.toList()))
                .agg(max("gross_currency_code"), max("gross_amount"));
        resultSet.persist();
        resultSet.write().format("org.apache.spark.sql.execution.datasources.hbase").save();
/*        Configuration hbaseConfiguration = HBaseConfiguration.create();
        JavaHBaseContext javaHBaseContext
                = new JavaHBaseContext(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), hbaseConfiguration);
        javaHBaseContext.hbaseContext().bulkPut(resultSet.rdd(), TableName.valueOf("MyTable"), new Function1<Row, Put>() {
            @Override
            public Put apply( Row v1) {
                return null;
            }

            public Put call(Row v) throws Exception {
                String[] part = v.toString().split(",");
                Put put = new Put(Bytes.toBytes(part[0]));

                put.addColumn(Bytes.toBytes(part[1]),
                        Bytes.toBytes(part[2]),
                        Bytes.toBytes(part[3]));
                return put;
            }

        });*/
        System.out.println("Number of rows are :" + resultSet.count());
      //  sc.close();
        sparkSession.close();
    }
}

        /*                = sparkSession
                .read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:HAWAAPP/HAWAPPSTG@msmaster-db-rw-a.qa.paypal.com:2126/SRV_MSM")
                .option("dbtable",typeQuery)
                .option("user","HAWAAPP")
                .option("password","HAWAAPPSTG")
                .option("customSchema", "SOURCE_TIME_CREATED decimal(38,0), CUSTOMER_ACTIVITY_KEY decimal(38,0), TRANSACTION_ACTION_ID decimal(38,0), TRANSACTION_ID decimal(38,0), PARENT_TRANSACTION_ID decimal(38,0), PARTNER_ACCOUNT_NUMBER decimal(38,0), MOD_PARTNER_ACCOUNT_NUMBER decimal(38,0), MERCHANT_ACCOUNT_NUMBER decimal(38,0), GROSS_AMOUNT decimal(28,10), FEE_AMOUNT decimal(28,10), COUNTER_ACCOUNT_NUMBER decimal(38,0), TRANSACTION_TIME_CREATED decimal(38,0), TRANSACTION_TIME_UPDATED decimal(38,0), FLAGS_01 decimal(38,0), FLAGS_02 decimal(38,0), FLAGS_03 decimal(38,0), FLAGS_04 decimal(38,0), FLAGS_05 decimal(38,0), FLAGS_06 decimal(38,0), FLAGS_07 decimal(38,0), SHARED_ID decimal(38,0), USD_MONEY_AMOUNT decimal(28,10), TRANSACTION_PAYMENT_ID decimal(38,0), PAYPAL_FIXED_FEE_AMOUNT decimal(28,10), PAYPAL_VAR_FEE_AMOUNT decimal(28,10), DISBURSEMENT_TIME decimal(38,0), DISBURSEMENT_AMOUNT decimal(28,10), TIME_CREATED decimal(38,0), TIME_UPDATED decimal(38,0), BASE_FEE_PERCENTAGE decimal(28,10), ACTUAL_FEE_PERCENTAGE decimal(28,10), BASE_VAR_FEE_MONEY_AMOUNT decimal(28,10), BASE_FIXED_FEE_MONEY_AMOUNT decimal(28,10)")
                .load();*/
