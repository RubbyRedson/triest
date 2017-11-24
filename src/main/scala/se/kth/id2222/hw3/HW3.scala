package se.kth.id2222.hw3

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object HW3 {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input = "./dataset/ca-AstroPh/out.ca-AstroPh"

    // get input data
    val edges: DataSet[String] =
      env.readTextFile(input)

    val counts = edges.flatMap { _.split("\t") }

    // Reservoir sampling
    val s = 10 // We want to maintain a sample of 50 random edges
    var n = 0  // How many we have encountered
    val rnd = new Random()

    val sample = new mutable.ArrayBuffer[String]()
    counts.map {edge =>
      n += 1
      if (sample.size < s) {
        sample.append(edge)
      }
      else {
        if (s.toDouble / n > rnd.nextDouble()) {
          sample.remove(rnd.nextInt(sample.size))
          sample.append(edge)
          sample.foreach(s => print(s + "\t"))
          println
        }
      }
    }
      .collect()

  }
}
