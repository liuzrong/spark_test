package example.graphx
import org.apache.spark._
import org.apache.spark.graphx._

object GraphxDemo {
	def main(args: Array[String]): Unit = {


		val conf = new SparkConf().setMaster("spark://master:7077").setAppName("Demo")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val sc = new SparkContext(conf)
		sc.addJar("local:/mnt/hgfs/idea/scala/spark_test/out/artifacts/spark_test_jar/spark_test.jar")


		case class User(name: String, occupation: String)

		val users = sc.textFile("hdfs://master:9000/input/scala_data/users.txt").map{line =>
			val fields = line.split(",")
			(fields(0).toLong,User(fields(1),fields(2)))
		}

		val friends = sc.textFile("hdfs://master:9000/input/scala_data/friends.txt").map{ line =>
			val fields = line.split(",")
			Edge(fields(0).toLong, fields(1).toLong, "friend")
		}


		val graph = Graph(users, friends)
		graph.vertices.collect
		graph.edges.collect

		graph.vertices.mapValues{(id,u) => u.name}.take(10).foreach(x=>println(x))
		graph.edges.mapValues(x => s"${x.srcId} -> ${x.dstId}").take(10)





	}

}
