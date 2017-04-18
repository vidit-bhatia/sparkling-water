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

package org.apache.spark.h2o.backends.external

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.SharedBackendConf

/**
  * External backend configuration
  */
trait ExternalBackendConf extends SharedBackendConf {
  self: H2OConf =>

  import ExternalBackendConf._

  def h2oCluster = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
  def h2oClusterHost = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(0))
  def h2oClusterPort = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(1).toInt)
  def h2oYARNQueue = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)
  def h2oDriverPath = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1)
  def h2oExternalNodesCount = sparkConf.getOption(PROP_EXTERNAL_H2O_NODES._1)
  def h2oHDFSOutputDir = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1)
  def h2oMapperXmx = sparkConf.get(PROP_EXTERNAL_H2O_MEMORY._1, PROP_EXTERNAL_H2O_MEMORY._2)
  def h2oClusterNotifyFile = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_NOTIFY_FILE._1)
  def h2oClusterStartMode = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1, PROP_EXTERNAL_CLUSTER_START_MODE._2)
  def isAutoH2OClusterStartEnabled = h2oClusterStartMode == "auto"
  def isManualH2OClusterStartEnabled = !isAutoH2OClusterStartEnabled
  def h2oClusterStartTimeout = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, PROP_EXTERNAL_CLUSTER_START_TIMEOUT._2)
  def clientConnectionTimeout = sparkConf.getInt(PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._1, PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._2)
  def clientCheckRetryTimeout = sparkConf.getInt(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._2)

  /**
    * Sets node and port representing H2O Cluster to which should H2O connect when started in external mode.
    * This method automatically sets external cluster mode
    *
    * @param host host representing the cluster
    * @param port port representing the cluster
    * @return H2O Configuration
    */
  def setH2OCluster(host: String, port: Int): H2OConf = {
    setExternalClusterMode()
    sparkConf.set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, host + ":" + port)
    self
  }

  def setH2OCluster(hostPort: String): H2OConf = {
    setExternalClusterMode()
    sparkConf.set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, hostPort)
    self
  }

  def setYARNQueue(queueName: String) : H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1, queueName)
    self
  }

  def setH2ODriverPath(path: String): H2OConf = {
    setExternalClusterMode()
    logWarning("Using external cluster mode!")
    sparkConf.set(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1, path)
    self
  }

  def setNumOfExternalH2ONodes(numOfExternalH2ONodes: Int): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_H2O_NODES._1, numOfExternalH2ONodes.toString)
    self
  }

  def setHDFSOutputDir(dir: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1, dir)
    self
  }

  def setMapperXmx(mem: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_H2O_MEMORY._1, mem)
    self
  }

  def setClusterConfigFile(path: String) : H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_NOTIFY_FILE._1, path)
    self
  }

  def useAutoClusterStart() : H2OConf = {
    setExternalClusterMode()
    logWarning("Using external cluster mode!")
    sparkConf.set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "auto")
    self
  }

  def useManualClusterStart() : H2OConf = {
    setExternalClusterMode()
    logWarning("Using external cluster mode!")
    sparkConf.set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "manual")
    self
  }

  def setClusterStartTimeout(clusterStartTimeout: Int): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, clusterStartTimeout.toString)
    self
  }

  def setClientConnectionTimeout(timeout: Int): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._1, timeout.toString)
    self
  }

  def setClientCheckRetryTimeout(timeout: Int): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, timeout.toString)
    self
  }

  def externalConfString: String =
    s"""Sparkling Water configuration:
        |  backend cluster mode : ${backendClusterMode}
        |  cluster start mode   : ${h2oClusterStartMode}
        |  cloudName            : ${clusterName.getOrElse("Not set yet")}
        |  cloud representative : ${h2oCluster.getOrElse("Not set, using cloud name only")}
        |  clientBasePort       : ${clientBasePort}
        |  h2oClientLog         : ${clientLogLevel}
        |  nthreads             : ${nthreads}""".stripMargin
}

object ExternalBackendConf {

  /** ip:port of arbitrary h2o node to identify external h2o cluster */
  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE = ("spark.ext.h2o.cloud.representative", None)

  /** Yarn queue on which external cluster should be started */
  val PROP_EXTERNAL_CLUSTER_YARN_QUEUE = ("spark.ext.h2o.external.yarn.queue", None)

  /** Path to h2o driver */
  val PROP_EXTERNAL_CLUSTER_DRIVER_PATH = ("spark.ext.h2o.external.h2o.driver", None)

  /** Number of nodes to wait for when connecting to external H2O cluster */
  val PROP_EXTERNAL_H2O_NODES = ("spark.ext.h2o.external.cluster.num.h2o.nodes", None)

  /** Location of HDFS directory for h2o started on yarn */
  val PROP_EXTERNAL_CLUSTER_HDFS_DIR = ("spark.ext.h2o.external.hdfs.dir", None)

  /** Amount of memory assigned to each h2o node started on yarn */
  val PROP_EXTERNAL_H2O_MEMORY = ("spark.ext.h2o.hadoop.memory", "6g")

  /** Location of cluster notification file */
  val PROP_EXTERNAL_CLUSTER_NOTIFY_FILE = ("spark.ext.h2o.cluster.info.name", None)

  /**
    * If this option is set to "auto" then h2o external cluster will be automatically started using the provided
    * h2o driver on yarn, otherwise it is expected that the cluster will be started by the user
    */
  val PROP_EXTERNAL_CLUSTER_START_MODE = ("spark.ext.h2o.external.start.mode", "manual")

  /** Timeout in seconds for starting h2o external cluster */
  val PROP_EXTERNAL_CLUSTER_START_TIMEOUT = ("spark.ext.h2o.cluster.start.timeout", 120)

  /** Timeout in milliseconds for watchdog client connection. If client is not connected
    * to the external cluster in the given time, the cluster is killed */
  val PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT = ("spark.ext.h2o.cluster.client.connect.timeout", 120000 + 10000)

  /** Timeout in milliseconds specifying how often the check for connected watchdog client is done */
  val PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT = ("spark.ext.h2o.cluster.client.retry.timeout", 10000)
}
