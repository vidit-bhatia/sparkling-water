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

package water

import java.io.File

import hex.svd.{SVD, SVDModel}
import org.apache.spark.h2o.{H2OConf, H2OContext, H2OFrame}
import org.apache.spark.{SparkConf, SparkFiles, SparkSessionUtils}

/**
  * A simple wrapper to allow launching H2O itself on the
  * top of Spark.
  */
object SparklingWaterDriver {

  /** Entry point */
  def main(args: Array[String]) {
    // Configure this application
    val conf: SparkConf = H2OConf.checkSparkConf(
      new SparkConf()
        .setAppName("Sparkling Water Driver")
        .setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local[*]"))
        .set("spark.ext.h2o.repl.enabled","true"))

    val spark = SparkSessionUtils.createSparkSession(conf)
    // Start H2O cluster only
    val hc = H2OContext.getOrCreate(spark.sparkContext)

    println(hc)

    spark.sparkContext.addFile("../iris_wheader.csv")

    val irisData = new H2OFrame(new File(SparkFiles.get("iris_wheader.csv")))

    val params = new SVDModel.SVDParameters()
    params._train = irisData._key
    params._valid = irisData._key
    import hex.DataInfo
    import hex.svd.SVDModel.SVDParameters
    params._nv = 4
    params._seed = 4
    params._only_v = false
    params._transform = DataInfo.TransformType.NONE
    params._svd_method = SVDParameters.Method.GramSVD
    params._save_v_frame = false
    params._keep_cross_validation_fold_assignment = true
    params._keep_cross_validation_predictions = true

    val rdd = spark.sparkContext.parallelize(List(1),1)

    val res = new SVD(params).trainModel().get()
    print(res)
  }
}
