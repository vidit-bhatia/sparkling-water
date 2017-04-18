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

package org.apache.spark.h2o.backends.internal

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.SharedBackendConf

/**
  * Internal backend configuration
  */
trait InternalBackendConf extends SharedBackendConf {
  self: H2OConf =>

  import InternalBackendConf._
  def numSparkExecutorHint = sparkConf.getOption(PROP_SPARK_EXECUTOR_HINT._1).map(_.toInt)
  def useFlatFile   = sparkConf.getBoolean(PROP_USE_FLATFILE._1, PROP_USE_FLATFILE._2)
  def nodeBasePort  = sparkConf.getInt(PROP_NODE_BASE_PORT._1, PROP_NODE_BASE_PORT._2)
  def discoveryRDDMulFactor = sparkConf.getInt(PROP_SPREADRDD_MUL_FACTOR._1, PROP_SPREADRDD_MUL_FACTOR._2)
  def spreadRDDNumRetries = sparkConf.getInt(PROP_SPREADRDD_RETRIES._1, PROP_SPREADRDD_RETRIES._2)
  def spreadRDDSubseqTries  = sparkConf.getInt(PROP_SPREADRDD_SUBSEQ_TRIES._1, PROP_SPREADRDD_SUBSEQ_TRIES._2)
  def nodeNetworkMask   = sparkConf.getOption(PROP_NODE_NETWORK_MASK._1)
  def nodeIcedDir   = sparkConf.getOption(PROP_NODE_ICED_DIR._1)

  def setNumSparkExecutorHint(hint: Int): H2OConf = {
    sparkConf.set(PROP_SPARK_EXECUTOR_HINT._1, hint.toString)
    self
  }

  def setUseFlatFile(useFlatFile: Boolean): H2OConf = {
    sparkConf.set(PROP_USE_FLATFILE._1, useFlatFile.toString)
    self
  }

  def setNodeBasePort(nodeBasePort: Int): H2OConf = {
    sparkConf.set(PROP_NODE_BASE_PORT._1, nodeBasePort.toString)
    self
  }

  def setSpreadRDDMulFactor(factor: Int): H2OConf = {
    sparkConf.set(PROP_SPREADRDD_MUL_FACTOR._1, factor.toString)
    self
  }

  def setSpreadRDDNumRetries(retries: Int): H2OConf = {
    sparkConf.set(PROP_SPREADRDD_RETRIES._1, retries.toString)
    self
  }

  def setSpreadRDDSubseqTries(subseqTries: Int): H2OConf = {
    sparkConf.set(PROP_SPREADRDD_SUBSEQ_TRIES._1, subseqTries.toString)
    self
  }

  def setNodeNetworkMask(nodeNetworkMask: String): H2OConf = {
    sparkConf.set(PROP_NODE_NETWORK_MASK._1, nodeNetworkMask)
    self
  }

  def setNodeIcedDir(nodeIcedDir: String): H2OConf = {
    sparkConf.set(PROP_NODE_ICED_DIR._1, nodeIcedDir)
    self
  }

  def internalConfString: String =
    s"""Sparkling Water configuration:
        |  backend cluster mode : ${backendClusterMode}
        |  workers              : ${numSparkExecutorHint}
        |  cloudName            : ${cloudName.getOrElse("Not set yet, it will be set automatically before starting H2OContext.")}
        |  flatfile             : ${useFlatFile}
        |  clientBasePort       : ${clientBasePort}
        |  nodeBasePort         : ${nodeBasePort}
        |  cloudTimeout         : ${clientCloudConnectTimeout}
        |  h2oNodeLog           : ${h2oNodeLogLevel}
        |  h2oClientLog         : ${h2oClientLogLevel}
        |  nthreads             : ${nthreads}
        |  drddMulFactor        : $discoveryRDDMulFactor""".stripMargin

}

object InternalBackendConf {

  /**
    * Expected number of spark executors.
    * Value None means automatic detection of cluster size.
    */
  val PROP_SPARK_EXECUTOR_HINT = ("spark.ext.h2o.cluster.size", None)

  /** Use flatfile for H2O cloud formation. */
  val PROP_USE_FLATFILE = ("spark.ext.h2o.flatfile", true)

  /** Base port used for individual H2O nodes configuration. */
  val PROP_NODE_BASE_PORT = ( "spark.ext.h2o.node.port.base", 54321)

  /** Multiplication factor for spread RDD generation.
    * Size of dummy RDD is PROP_CLUSTER_SIZE*PROP_DUMMY_RDD_MUL_FACTOR */
  val PROP_SPREADRDD_MUL_FACTOR = ("spark.ext.h2o.dummy.rdd.mul.factor", 10)

  /** Number of retries to create an spread RDD spread over all executors */
  val PROP_SPREADRDD_RETRIES = ("spark.ext.h2o.spreadrdd.retries", 10)

  /** Subsequent successful tries to figure out size of Spark cluster which are producing same number of nodes. */
  val PROP_SPREADRDD_SUBSEQ_TRIES = ("spark.ext.h2o.subseq.tries", 5)

  /** Subnet selector for H2O nodes running inside executors - if the mask is specified then Spark network setup is not discussed. */
  val PROP_NODE_NETWORK_MASK = ("spark.ext.h2o.node.network.mask", None)

  /** Location of iced directory for Spark nodes */
  val PROP_NODE_ICED_DIR = ("spark.ext.h2o.node.iced.dir", None)
}
