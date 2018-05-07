package example.rdd

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object RDDDemo {


	def seqOp(a:(Int,Int), b:Int) : (Int,Int) = {
		 //println("seqOp: " + a._1 + "\t" +a._2 + "\t" + b)
		(a._1 + b,a._2+ 1)
	}


	def combOp(a:(Int,Int), b:(Int,Int)) : (Int,Int) = {
		println("combOp: " + a + "\t" + b)
		(a._1 + b._1, a._2 + b._2)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("spark://master:7077").setAppName("Demo")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val sc = new SparkContext(conf)
		sc.addJar("local:/mnt/hgfs/idea/scala/spark_test/out/artifacts/spark_test_jar/spark_test.jar")


		//1、并行创建RDD
		val rdd1 = sc.parallelize(Seq(8,2,3,4))
		//rdd1.take(2).foreach(x=>println(x))


		//读取文件
		val rdd2 = sc.textFile("hdfs://master:9000/input/The_Man_of_Property.txt")
		//println(rdd2.count())
		//println(rdd2.first())

		rdd2.flatMap(x => x.split(" ")).take(10).foreach(x=>println(x))

		//转换函数
		val rdd1_map = rdd1.map(x=>x*x)
		//rdd1_map.take(2).foreach(x=>println(x))


		//SequenceFileRDD
		val rdd3=sc.textFile("hdfs://master:9000/input/scala_data/statesPopulation.csv")
		val rdd31=rdd3.map(record=>(record.split(",")(0),record.split(",")(2)))
		//rdd31.take(10).foreach(x=>println(x))
		//rdd31.saveAsSequenceFile("hdfs://master:9000/seqFile")

		val seqRDD=sc.sequenceFile[String,String]("hdfs://master:9000/seqFile")
		//seqRDD.take(10).foreach(x=>println(x))


		//	CoGroupedRDD 两个Rdd中相同key的value合并成一个集合
		val rdd32=rdd3.map(record=>(record.split(",")(0),record.split(",")(1)))
		//rdd32.take(10).foreach(x=>println(x))
		val rdd33 = rdd31.cogroup(rdd32)
		//rdd33.take(10).foreach(x=>println(x))


		// ShuffledRDD
		val rdd34 = rdd3.map(record=> (record.split(",")(0),1))
		val rdd35 = rdd34.reduceByKey(_+_)
		//rdd35.take(5).foreach(x=>println(x))


		val rdd4 = sc.textFile("hdfs://master:9000/input/scala_data/statesPopulation.csv")

		//转换为键值对
		val rdd41 = rdd4.map(record => record.split(",")).map(t => (t(0), (t(1).toInt, t(2).toInt)))
		//rdd41.take(10).foreach(x =>println(x))

		//groupByKey
		val rdd42 = rdd41.groupByKey.map(x => {var sum=0; x._2.foreach(sum += _._2); (x._1, sum)})
        //rdd42.take(10).foreach(x=>println(x))


		//reduceByKey
		val rdd43 = rdd41.reduceByKey((x, y) => (x._2,x._2+y._2)).map(x=> (x))
		//rdd43.take(10).foreach(x=> println(x))


		val x = sc.parallelize(Array(("a", ("z",1)),("a", ("x",1)),("a", ("m",1)), ("b", ("x",1)), ("a",("y",1))))

		val y = x.reduceByKey((a,b) =>(a._1,a._2+b._2)).collect()
		//y.take(10).foreach(x=>println(x))


		/**
		  * aggregate
		  * 分为两步：
		  * 1、在各个分区中做聚合
		  * 2、将各个分区中的相同key聚合在一起
	    */
		val rdd5 = sc.parallelize(List(1,2,3,4,5,6,7,8,9),3)
		/*val rdd51 = rdd5.aggregate((0,0))(
			seqOp,
			combOp
		)*/

		//println(rdd51)


		/**
		  * aggregateByKey
		  * 输入： （state，（year,peoples））
		  * 输出： （state, sum(peoples)）
	    * */
		val seqOp2 = (s:Int,v:(Int,Int)) => (s+v._2)

		val combiner2 = (v1:Int ,v2:Int) => (v1 + v2)

		val rdd44 = rdd41.aggregateByKey(0,3)(
			seqOp2,
			combiner2
		)

		//rdd44.take(5).foreach(x => println(x))


		/**
		  * combiner
		  * 输入： （state，（year,peoples）
		  * 输出： （state, sum(peoples)）
		  * createCombiner: V => C ，初始化操作,可以对初始值进行类型转换等
		    mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上
									(这个操作在每个分区内进行)
		    mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)
	    */

		val createCombiner = (v: (Int,Int)) => v._2
		val mergeValue = (c: Int, v:(Int,Int)) => (c + v._2)
		val mergeCombiner = (c1 : Int, c2 : Int) => c1 + c2


		val rdd45 = rdd41.combineByKey(
			createCombiner,
			mergeValue,
			mergeCombiner
		)

		//rdd45.take(10).foreach(x => println(x))



		//RangePartitioner
		val pairRdd = sc.parallelize(List((1,1), (5,10), (5,9), (2,4), (3,5), (3,6),(4,7), (4,8),(2,3), (1,2)))

		val partitioned = pairRdd.partitionBy(new RangePartitioner(3,pairRdd))

		//partitioned.mapPartitionsWithIndex((i,x) => Iterator(""+i	+	":"+x.length)).take(10).foreach(x=>println(x))


		//create broadcast variables
		val	rdd_one	=	sc.parallelize(Seq(1,2,3))

		val a = 5

		val bi = sc.broadcast(a)


		//把broadcast的值从内存中移除，下次使用时重新加载，会缓存在driver中
		//bi.unpersist()


		//彻底销毁boradcast的值
		//bi.destroy()

		val rdd_1 = rdd_one.map((x) => bi.value + x)
		rdd_1.take(10).foreach(x => println(x))





		val map = mutable.HashMap(1->1,2->2,3->3)
		val mi = sc.broadcast(map)

		val rdd_2 = rdd_one.map((x) => x*mi.value(x))
		//rdd_2.take(10).foreach(x =>println(x))



		//Accumulator: 累加器
		val statesPopulationRDD = sc.textFile("hdfs://master:9000/input/scala_data/statesPopulation.csv")

		statesPopulationRDD.take(5)

		val acc1 = sc.longAccumulator("acc1")
		val someRDD = statesPopulationRDD.map(x => {acc1.add(1); x})
		//println(acc1.value)
		//println(someRDD.count)
		//println(acc1.value)
		//println(acc1)












	}

}
