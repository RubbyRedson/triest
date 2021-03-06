package se.kth.id2222.hw3

import java.math.BigInteger

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
    val s = 2000 // Sample size, corresponds to M in the paper
    var n = 0 // How many we have encountered
    val rnd = new Random()

    //Triest
    var counters = new mutable.HashMap[String, Int]()
    var globalSeen = 0
    var globalEstimate : BigInteger = BigInteger.ZERO
    val graph = new mutable.HashMap[String, mutable.HashSet[String]]()

    val sample = new mutable.ArrayBuffer[String]()
    counts.map { edge =>
      n += 1 // Is denoted as t in the paper
      if (sample.size < s) {
        sample.append(edge)
        val newEstimates = updateCounters(edge, counters, globalSeen, graph)
        globalSeen = newEstimates._1
        counters = newEstimates._2
      }
      else {
        if (s.toDouble / n > rnd.nextDouble()) {
          val edgeToRemove = rnd.nextInt(sample.size)
          val estimatesAfterDeletion = updateCountersEdgeDeletion(sample(edgeToRemove), counters, globalSeen, graph)
          globalSeen = estimatesAfterDeletion._1
          counters = estimatesAfterDeletion._2

          sample.remove(edgeToRemove)
          sample.append(edge)
          val newEstimates = updateCounters(edge, counters, globalSeen, graph)
          globalSeen = newEstimates._1
          counters = newEstimates._2
        }
      }

      if (sample.size > 3) {
        var tmp : java.math.BigInteger = new BigInteger(n.toString).multiply(new BigInteger((n - 1).toString))
          .multiply(new BigInteger((n - 2).toString)).divide(new BigInteger((sample.size).toString))
          .divide(new BigInteger((sample.size - 1).toString)).divide(new BigInteger((sample.size - 2).toString))

        tmp = tmp.max(BigInteger.ONE)
        globalEstimate = tmp.multiply(new BigInteger(globalSeen.toString))
      }
      if (n % 40000 == 0) {
        println("n: " + n + "\nGlobal seen: " + globalSeen + "\nGlobal estimate: " + globalEstimate + "\n--------")
      }

    }
      .collect()

  }

  // Triest Base
  def updateCounters(newEdge: String, counters: mutable.HashMap[String, Int],
                     globalSeen: Int, graph: mutable.HashMap[String, mutable.HashSet[String]]): (Int, mutable.HashMap[String, Int]) = {
    val edges = newEdge.split(" ")
    val (node1, node2) = (edges(0).trim, edges(1).trim) // (1, 2)
    var newGlobal = globalSeen

    if (graph.contains(node1)) graph(node1).add(node2) // {1 : {3, 4}}
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

  def updateCountersEdgeDeletion(newEdge: String, counters: mutable.HashMap[String, Int],
                                 globalSeen: Int, graph: mutable.HashMap[String, mutable.HashSet[String]]): (Int, mutable.HashMap[String, Int]) = {
    val edges = newEdge.split(" ")
    val (node1, node2) = (edges(0).trim, edges(1).trim)
    var newGlobal = globalSeen

    val neighbours1 = graph(node1)
    val neighbours2 = graph(node2)
    val neigbourhood = neighbours1.intersect(neighbours2)

    neigbourhood.foreach(mutualNeighbour => {
      newGlobal -= 1
      counters(mutualNeighbour) = counters(mutualNeighbour) - 1
      counters(node1) = counters(node1) - 1
      counters(node2) = counters(node2) - 1

      if (counters(mutualNeighbour) == 0) counters.remove(mutualNeighbour)
      if (counters(node1) == 0) counters.remove(node1)
      if (counters(node2) == 0) counters.remove(node2)
    })

    graph(node1).remove(node2)
    graph(node2).remove(node1)

    if (graph(node1).isEmpty) graph.remove(node1)
    if (graph(node2).isEmpty) graph.remove(node2)

    (newGlobal, counters)
  }
}
