package se.kth.id2222.hw3

import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random


object TriestBase {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input = "./dataset/ca-AstroPh/out.ca-AstroPh"

    // get input data
    val edges: DataSet[String] =
      env.readTextFile(input)

    val counts = edges.flatMap {
      _.split("\t")
    }

    // Reservoir sampling
    val s = 50 // Sample size
    var n = 0 // How many we have encountered
    val rnd = new Random()

    //Triest
    var counters = new mutable.HashMap[String, Int]()
    var globalSeen = 0
    var globalEstimate : BigInt = 0
    val graph = new mutable.HashMap[String, mutable.HashSet[String]]()

    val sample = new mutable.ArrayBuffer[String]()
    counts.map { edge =>
      n += 1
      if (sample.size < s) {
        sample.append(edge)
        val newEstimates = updateCounters(edge, counters, globalSeen, graph)
        globalSeen = newEstimates._1
        counters = newEstimates._2
        //        println("Global estimate: " + globalEstimate)
        //        println("Local estimates: " )
        //        counters.foreach { case (node, counter) => print(node + " : " + counter + "\t") }
      }
      else {
        if (s.toDouble / n > rnd.nextDouble()) {
          sample.remove(rnd.nextInt(sample.size))
          sample.append(edge)
          val newEstimates = updateCounters(edge, counters, globalSeen, graph)
          globalSeen = newEstimates._1
          counters = newEstimates._2
          //          println("Global estimate: " + globalEstimate)
          //          println("Local estimates: " + counters.foreach { case (node, counter) => print(node + " : " + counter + "\t") })
        }
      }

      if (sample.size > 3) {
        var tmp : BigInt = n / sample.size * (n - 1) / (sample.size - 1) * (n - 2)  / (sample.size - 2)
        if (tmp < 1) tmp = 1
        globalEstimate = tmp * globalSeen
      }
      if (n % 10000 == 0) {
        println("n: " + n + "\nGlobal seen: " + globalSeen + "\nGlobal estimate: " + globalEstimate + "\n--------")
      }

    }
      .collect()

  }

  // Triest Base
  def updateCounters(newEdge: String, counters: mutable.HashMap[String, Int],
                     globalEstimate: Int, graph: mutable.HashMap[String, mutable.HashSet[String]]): (Int, mutable.HashMap[String, Int]) = {
    val edges = newEdge.split(" ")
    val (node1, node2) = (edges(0).trim, edges(1).trim)
    var newGlobal = globalEstimate

    if (graph.contains(node1)) graph(node1).add(node2)
    else {
      val set = new mutable.HashSet[String]()
      set.add(node2)
      graph.put(node1, set)
    }

    if (graph.contains(node2)) graph(node2).add(node1)
    else {
      val set = new mutable.HashSet[String]()
      set.add(node1)
      graph.put(node2, set)
    }

    val neighbours1 = graph(node1)
    val neighbours2 = graph(node2)
    val neigbourhood = neighbours1.intersect(neighbours2)

    neigbourhood.foreach(mutualNeighbour => {
      newGlobal += 1
      if (counters.contains(mutualNeighbour)) {
        counters(mutualNeighbour) = counters(mutualNeighbour) + 1
      } else {
        counters(mutualNeighbour) = 1
      }

      if (counters.contains(node1)) {
        counters(node1) = counters(node1) + 1
      } else {
        counters(node1) = 1
      }

      if (counters.contains(node2)) {
        counters(node2) = counters(node2) + 1
      } else {
        counters(node2) = 1
      }
    })

    (newGlobal, counters)
  }
}
