package example.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object DataFrameExample {

    def main(args: Array[String]): Unit = {

        //1、创建DataFrame
        val conf = new SparkConf().setMaster("spark://master:7077").setAppName("sql_demo")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
		sc.addJar("local:/mnt/hgfs/idea/scala/spark_test/out/artifacts/spark_test_jar/spark_test.jar")


		val warehouseLocation = "hdfs://master:9000/user/hive/warehouse"
		val spark = SparkSession
				.builder()
				.appName("Spark Hive Example")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.enableHiveSupport()
				.getOrCreate()


		//测试
		//spark.sql("select count(*) from bookcrossing.users").show(1)

		spark.conf.set("spark.sql.warehouse.dir","hdfs://master:9000/user/hive/warehouse")

        val priors = spark.sql("select * from orderproducts.priors")
		val orders = spark.sql("select * from orderproducts.orders")


		//1.1、统计product被购买的数据量
		/*      priors.groupBy("product_id")
					  .count()
					  .show(10)
		*/





		//1.2、product 被reordered（再次购买的数量）

		val po = priors.selectExpr("product_id","reordered").filter("reordered == 1")
			   .groupBy("product_id")
			   .sum("reordered")
			   .withColumnRenamed("sum(reordered)","reordered_sum")


		/*
		   1.3、计算product被再次购买的比率
		   比率 = 再次购买的数量/总购买次数
		 */

		/*priors.selectExpr("product_id","reordered")
		            .groupBy("product_id")
	        		.count()
				    .join(po,"product_id")
		            .selectExpr("product_id","reordered_sum/count")
				    .withColumnRenamed("(reordered_sum / count)","rate")
	        	    .show(10)*/


		//自定义udf
		val avg_udf = udf((sm:Long,cnt:Long)=>sm.toDouble/cnt.toDouble)

		//withColumn 添加或替换一列
		/*priors.selectExpr("product_id","reordered")
				.groupBy("product_id")
				.count()
				.join(po,"product_id")
				.withColumn("mean_re",avg_udf(col("reordered_sum"),col("count")))
				.show(10)*/








		/*2.1、每个用户平均购买订单的间隔周期
		   1、将days_since_prior_order 为空的置为0
		   2、求days_since_prior_order的平均值
		 */
		/*orders.selectExpr("*","if(days_since_prior_order='',0,days_since_prior_order) as dspo")
		        .groupBy("user_id")
	        	.avg("dspo")
				.orderBy("user_id")
				.show(10)*/




		//2.2、每个用户的总订单数量
		/*spark.sql("select * from orderproducts.orders")
			 .groupBy("user_id")
			 .count().show(10)*/




		//2.3、每个用户购买的商品去重后的集合

		//DataFrame转为RDD
		/*val rdd_records = orders.join(priors,"order_id")
				.selectExpr("user_id","product_id")
				.rdd.map(x=>(x(0).toString,x(1).toString))
				.groupByKey()
				.mapValues(_.toSet.mkString(","))*/


		//将rdd转换为DataFrame
		/*import spark.implicits._
		rdd_records.toDF("user_id","product_records").show(10,false)*/












		//2.4、每个用户总商品数量以及去重后的数量

		//总商品数量
		/*val rdd_records = orders.join(priors,"order_id")
				.selectExpr("user_id","product_id")
				.rdd.map(x=>(x(0).toString,x(1).toString))
				.groupByKey()
				.mapValues(_.toSet.size)
	        	.toDF("user_id","product_records")*/

		/*orders.join(priors,"order_id")
				.selectExpr("user_id","product_id")
				.groupBy("user_id")
				.count()
	        	.join(rdd_records,"user_id")
				.show(50)*/


		//2.5、每个用户购买的平均每个订单的商品数量

		val pr = priors.selectExpr("order_id")
		         .groupBy("order_id")
	        	 .count()
		orders.join(pr,"order_id")
				.groupBy("user_id")
	        	.agg(avg("count"))
	        	.show(10)



    }
}
