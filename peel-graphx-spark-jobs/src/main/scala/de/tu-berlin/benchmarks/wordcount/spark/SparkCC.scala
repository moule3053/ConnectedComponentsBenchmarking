/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package de.tu-berlin.benchmarks.wordcount.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

/**
 * A connected components algorithm example.
 * The connected components algorithm labels each connected component of the graph
 * with the ID of its lowest-numbered vertex.
 * For example, in a social network, connected components can approximate clusters.
 * GraphX contains an implementation of the algorithm in the
 * [`ConnectedComponents` object][ConnectedComponents],
 * and we compute the connected components of the example social network dataset.
 *
 */
object SparkCC {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
        Console.err.println("Usage: <jar> inputPath outputPath")
        System.exit(-1)
     }

      val inputPath = args(0)
      val outputPath = args(1)
    // Creates a SparkSession.
    val spark = SparkSession
    .builder
    .appName(s"${this.getClass.getSimpleName}")
    .getOrCreate()
    val sc = spark.sparkContext
    //val sc = new SparkContext(new SparkConf().setAppName(s"${this.getClass.getSimpleName}"))
    //val sc: SparkContext

    // $example on$
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, inputPath)
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Print the result
    //println(ccByUsername.collect().mkString("\n"))
    cc.saveAsTextFile(outputPath)
    // $example off$
    spark.stop()
  }
}
// scalastyle:on println
