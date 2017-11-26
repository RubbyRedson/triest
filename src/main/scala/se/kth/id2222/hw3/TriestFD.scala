package se.kth.id2222.hw3

import java.math.BigInteger

import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random


object TriestFD {
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
    val rnd = new Random()
    val s = 2000 // Sample size
    var n = 0 // How many we have encountered


    //Triest
    var counters = new mutable.HashMap[String, BigInteger]()
    var globalEstimate: BigInteger = BigInteger.ZERO
    val graph = new mutable.HashMap[String, mutable.HashSet[String]]()

    var sTr = 0
    var di = 0
    var d0 = 0

    var sample = new mutable.ArrayBuffer[String]()
    counts.map { edge =>
      n += 1
      sTr += 1

      if (true) { //current dataset contains only insertions
        if (di + d0 == 0) {
          val newEstimates = updateCounters(edge, counters, globalEstimate, graph, n, sample.size, di, d0)
          globalEstimate = newEstimates._1
          counters = newEstimates._2
          if (sample.size < s) {
            sample.append(edge)
          }
          else {
            if (s.toDouble / n > rnd.nextDouble()) {
              val edgeToRemove = rnd.nextInt(sample.size)

              val newEstimates = updateCountersEdgeDeletion(sample(edgeToRemove), counters, globalEstimate, graph, n, sample.size, di, d0)
              globalEstimate = newEstimates._1
              counters = newEstimates._2

              sample.remove(edgeToRemove)
              sample.append(edge)
            }
          }
        } else if (di.toDouble / (di + d0) > rnd.nextDouble()) {
          di -= 1
        } else {
          d0 -= 1
        }
      } else if (counters.contains(edge.split(" ")(0).trim) && counters.contains(edge.split(" ")(1).trim)) {

        val newEstimates = updateCountersEdgeDeletion(edge, counters, globalEstimate, graph, n, sample.size, di, d0)
        globalEstimate = newEstimates._1
        counters = newEstimates._2
        sample = sample.filter(el => el.ne(edge))
        di += 1
      } else {
        d0 += 1
      }
      if (n % 40000 == 0) {
        println("n: " + n + "\nGlobal estimate: " + globalEstimate + "\n--------")
      }

    }
      .collect()

  }

  // Triest FD
  def updateCounters(newEdge: String, counters: mutable.HashMap[String, BigInteger],
                     globalEstimate: BigInteger, graph: mutable.HashMap[String, mutable.HashSet[String]],
                     n: Int, m: Int, di: Int, d0: Int): (BigInteger, mutable.HashMap[String, BigInteger]) = {
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
      var coeff : BigInteger = new BigInteger((n - 1).toString).multiply(new BigInteger((n - 2).toString))
        .divide(new BigInteger((m).toString)).divide(new BigInteger((m-1).toString))  // compute the nu (weighted increase)
      coeff = coeff.max(BigInteger.ONE)


      neigbourhood.foreach(mutualNeighbour => {
        newGlobal = newGlobal.add(coeff)
        if (counters.contains(mutualNeighbour)) {
          counters(mutualNeighbour) = counters(mutualNeighbour).add(coeff)
        } else {
          counters(mutualNeighbour) = coeff
        }

        if (counters.contains(node1)) {
          counters(node1) = counters(node1).add(coeff)
        } else {
          counters(node1) = coeff
        }

        if (counters.contains(node2)) {
          counters(node2) = counters(node2).add(coeff)
        } else {
          counters(node2) = coeff
        }
      })
    }
    (newGlobal, counters)
  }

  def updateCountersEdgeDeletion(newEdge: String, counters: mutable.HashMap[String, BigInteger],
                                 globalSeen: BigInteger, graph: mutable.HashMap[String, mutable.HashSet[String]],
                                 n: Int, m: Int, di: Int, d0: Int): (BigInteger, mutable.HashMap[String, BigInteger]) = {
    val edges = newEdge.split(" ")
    val (node1, node2) = (edges(0).trim, edges(1).trim)
    var newGlobal = globalSeen

    val neighbours1 = graph(node1)
    val neighbours2 = graph(node2)
    val neigbourhood = neighbours1.intersect(neighbours2)
    if (m > 2 && n > 2) {
      var coeff : BigInteger = new BigInteger((n - 1).toString).multiply(new BigInteger((n - 2).toString))
        .divide(new BigInteger((m).toString)).divide(new BigInteger((m-1).toString))  // compute the nu (weighted increase)
      coeff = coeff.max(BigInteger.ONE)

      neigbourhood.foreach(mutualNeighbour => {
        newGlobal = newGlobal.subtract(coeff)
        if (counters.contains(mutualNeighbour)) {
          counters(mutualNeighbour) = counters(mutualNeighbour).subtract(coeff)
          if (counters(mutualNeighbour).equals(BigInteger.ZERO)) counters.remove(mutualNeighbour)
        }
        if (counters.contains(node1)) {
          counters(node1) = counters(node1).subtract(coeff)
          if (counters(node1).equals(BigInteger.ZERO)) counters.remove(node1)
        }
        if (counters.contains(node2)) {
          counters(node2) = counters(node2).subtract(coeff)
          if (counters(node2).equals(BigInteger.ZERO)) counters.remove(node2)
        }
      })
    }

    graph(node1).remove(node2)
    graph(node2).remove(node1)

    if (graph(node1).isEmpty) graph.remove(node1)
    if (graph(node2).isEmpty) graph.remove(node2)

    (newGlobal, counters)
  }
}
