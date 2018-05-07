package example.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("spark://master:7077").setAppName("Demo")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val sc = new SparkContext(conf)
		sc.addJar("local:/mnt/hgfs/idea/scala/spark_test/out/artifacts/spark_test_jar/spark_test.jar")


		val lines = sc.textFile("hdfs://master:9000/input/The_Man_of_Property.txt")



		/**
		  * 求和：
		  * 1、sum:  x._2.map(x=>x._2).sum
		  * 2、reduce: x._2.map(x=>x._2).reduce((a,b) => a+b)
		  */
		/*lines.flatMap(_.split(" "))
				.map((_,1))
				.groupBy(x=>x._1)
				.map(x=>(x._1, x._2.map(_._2).reduce(_+_)))
				.take(10)
				.foreach(x=>println(x))*/


		/**
		  * 排序：
		  * sortBy(_._2,false) 降序
		  */

		lines.flatMap(_.split(" "))
				.map((_,1))
				.groupBy(x=>x._1)
				.map(x=>(x._1, x._2.map(_._2).reduce(_+_)))
				.sortBy(_._2,false)
				.take(10)
				.foreach(x=>println(x))

		/**
		  * mapValues: 输入的数据为key-value
		  */
		/*lines.flatMap(_.split(" "))
				.map((_,1))
				.groupBy(x=>x._1)
				.mapValues(_.size)
	        	.take(5)
	        	.foreach(x=>println(x._2))*/


		/**
		  * foldLeft sum 区别：
		  * foldLeft可以设置一个基准，从左向右依次相加
		  */
		/*lines.flatMap(_.split(" "))
				.map((_,1))
				.groupBy(x=>x._1)
				.map(x=>(x._1,x._2.map(_._2).fold(0)(_+_)))
				.take(10)
				.foreach(x=>println(x))*/


		/**
		  * 正则表达式
		  */
		val p = "[0-9a-zA-Z]+".r

		/*lines.flatMap(_.split(" "))
				.map(x=> (p.findAllIn(x).mkString(""),1))
				.groupBy(x=>x._1)
				.map(x=>(x._1,x._2.map(_._2).fold(0)(_+_)))
				.take(10)
				.foreach(x=>println(x))*/














	}

}
