package se.kth.id2222.hw3

import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random


object TriestImpr {
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
    var counters = new mutable.HashMap[String, BigInt]()
    var globalEstimate: BigInt = 0
    val graph = new mutable.HashMap[String, mutable.HashSet[String]]()

    val sample = new mutable.ArrayBuffer[String]()
    counts.map { edge =>
      n += 1

      val newEstimates = updateCounters(edge, counters, globalEstimate, graph, n, sample.size)
      globalEstimate = newEstimates._1
      counters = newEstimates._2

      if (sample.size < s) {
        sample.append(edge)
      }
      else {
        if (s.toDouble / n > rnd.nextDouble()) {
          sample.remove(rnd.nextInt(sample.size))
          sample.append(edge)
        }
      }

      if (n % 10000 == 0) {
        println("n: " + n + "\nGlobal estimate: " + globalEstimate + "\n--------")
      }

    }
      .collect()

  }

  // Triest Improved
  def updateCounters(newEdge: String, counters: mutable.HashMap[String, BigInt],
                     globalEstimate: BigInt, graph: mutable.HashMap[String, mutable.HashSet[String]],
                     n: Int, m: Int): (BigInt, mutable.HashMap[String, BigInt]) = {
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

    if (m > 2 && n > 2) {
      var coeff = (n - 1) / (m - 1) * (n - 2) / m // compute the nu (weighted increase)
      if (coeff < 1) coeff = 1

      neigbourhood.foreach(mutualNeighbour => {
        newGlobal += coeff
        if (counters.contains(mutualNeighbour)) {
          counters(mutualNeighbour) = counters(mutualNeighbour) + coeff
        } else {
          counters(mutualNeighbour) = coeff
        }

        if (counters.contains(node1)) {
          counters(node1) = counters(node1) + coeff
        } else {
          counters(node1) = coeff
        }

        if (counters.contains(node2)) {
          counters(node2) = counters(node2) + coeff
        } else {
          counters(node2) = coeff
        }
      })
    }
    (newGlobal, counters)
  }
}
