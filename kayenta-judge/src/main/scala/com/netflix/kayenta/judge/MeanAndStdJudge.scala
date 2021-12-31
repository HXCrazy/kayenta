/*
 * Copyright 2021 Huami, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.kayenta.judge

import com.netflix.kayenta.canary.{CanaryClassifierThresholdsConfig, CanaryConfig, CanaryJudge}
import com.netflix.kayenta.canary.results.{CanaryAnalysisResult, CanaryJudgeResult}
import com.netflix.kayenta.judge.classifiers.metric.{Error, MannWhitneyClassifier, MeanAndStdClassifier, MetricDirection, NaNStrategy, Nodata, NodataFailMetric, OutlierStrategy}
import com.netflix.kayenta.judge.preprocessing.Transforms
import com.netflix.kayenta.judge.scorers.ScoringHelper
import com.netflix.kayenta.judge.stats.DescriptiveStatistics
import com.netflix.kayenta.judge.utils.MapUtils
import com.netflix.kayenta.metrics.MetricSetPair
import com.typesafe.scalalogging.StrictLogging
import org.springframework.stereotype.Component

import java.util
import java.util.stream.Collectors
import scala.collection.JavaConverters._

@Component
class MeanAndStdJudge extends CanaryJudge with StrictLogging{
  private final val judgeName = "MeanAndStdJudge-v1.0"
  override def getName: String = judgeName

  override def judge(canaryConfig: CanaryConfig, orchestratorScoreThresholds: CanaryClassifierThresholdsConfig, metricSetPairList: util.List[MetricSetPair]): CanaryJudgeResult = {
    val reqCountMetricOption = metricSetPairList.stream().filter(metricSetPair => Integer.valueOf(0).equals(metricSetPair.getAlgorithmType)).findFirst()
    val reqCount = getReqCount(reqCountMetricOption)
    var metricResults:List[CanaryAnalysisResult] = List()
    if (!reqCount.isNaN && reqCount <=0 ){
      metricResults = metricResults:+bulidFailureAnalysisResult(reqCountMetricOption, canaryConfig)
    }else{
      val analysisMetricPairList:util.List[MetricSetPair] = metricSetPairList.stream().filter(metricSetPair => null == metricSetPair.getAlgorithmType).collect(Collectors.toList())
      //Metric Classification
      metricResults = analysisMetricPairList.asScala.toList.map { metricPair =>
        classifyMetric(canaryConfig, metricPair, reqCount)
      }
    }

    val scoringHelper = new ScoringHelper(judgeName)
    scoringHelper.score(canaryConfig, orchestratorScoreThresholds, metricResults)
  }

  def bulidFailureAnalysisResult(reqCountMetricOption: util.Optional[MetricSetPair], canaryConfig: CanaryConfig):CanaryAnalysisResult={
    val metricPair = reqCountMetricOption.get()
    val metricConfig = canaryConfig.getMetrics.asScala.find(m => m.getName == metricPair.getName) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException(s"Could not find metric config for ${metricPair.getName}")
    }
    CanaryAnalysisResult.builder()
      .name(metricPair.getName)
      .id(metricPair.getId)
      .tags(metricPair.getTags)
      .classification(NodataFailMetric.toString)
      .groups(metricConfig.getGroups)
      .build()
  }

  def getReqCount(reqCountMetricOption: util.Optional[MetricSetPair]):Double={
    var reqCount = Double.NaN
    if (reqCountMetricOption.isPresent){
      val reqCountGroupList = reqCountMetricOption.get().getValues.values()
      var reqCountSum = 0
      reqCountGroupList.stream().forEach(reqCountGroup => {
        reqCountGroup.forEach(reqCountTemp =>{
          reqCountSum += reqCountTemp.toInt
        })
      })
      reqCount = new java.math.BigDecimal(reqCountSum).divide(new java.math.BigDecimal(2)).doubleValue()
    }
    reqCount
  }

  def handleNaNs(metric: Metric, nanStrategy: NaNStrategy): Metric = {
    if (nanStrategy == NaNStrategy.Remove) {
      Transforms.removeNaNs(metric)
    } else {
      Transforms.replaceNaNs(metric)
    }
  }

  def classifyMetric(canaryConfig: CanaryConfig, metric: MetricSetPair, reqCount: Double): CanaryAnalysisResult ={

    val metricConfig = canaryConfig.getMetrics.asScala.find(m => m.getName == metric.getName) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException(s"Could not find metric config for ${metric.getName}")
    }

    val experimentValues = metric.getValues.get("experiment").asScala.map(_.toDouble).toArray
    val controlValues = metric.getValues.get("control").asScala.map(_.toDouble).toArray

    val experiment = Metric(metric.getName, experimentValues, label="Canary")
    val control = Metric(metric.getName, controlValues, label="Baseline")

    val directionalityString = MapUtils.getAsStringWithDefault("either", metricConfig.getAnalysisConfigurations, "canary", "direction")
    val directionality = MetricDirection.parse(directionalityString)

    val nanStrategyString = MapUtils.getAsStringWithDefault("none", metricConfig.getAnalysisConfigurations, "canary", "nanStrategy")
    val nanStrategy = NaNStrategy.parse(nanStrategyString)

    //Effect Size Parameters
    val effectSizeMeasure = MapUtils.getAsStringWithDefault("meanRatio", metricConfig.getAnalysisConfigurations, "canary", "effectSize", "measure")
    val defaultEffect = if(effectSizeMeasure == "cles") 0.5 else 1.0

    val allowedIncrease = MapUtils.getAsDoubleWithDefault(defaultEffect, metricConfig.getAnalysisConfigurations, "canary", "effectSize", "allowedIncrease")
    val allowedDecrease = MapUtils.getAsDoubleWithDefault(defaultEffect, metricConfig.getAnalysisConfigurations, "canary", "effectSize", "allowedDecrease")
    val effectSizeThresholds = (allowedDecrease, allowedIncrease)

    val isCriticalMetric = true
    val isDataRequired = true
    val isMutedMetric = false

    //=============================================
    // Metric Transformation (Remove NaN values, etc.)
    // ============================================
    val transformedExperiment = handleNaNs(experiment, nanStrategy)
    val transformedControl = handleNaNs(control, nanStrategy)

    //=============================================
    // Calculate metric statistics
    // ============================================
    val experimentStats = DescriptiveStatistics.summary(transformedExperiment)
    val controlStats = DescriptiveStatistics.summary(transformedControl)

    //=============================================
    // Metric Classification
    // ============================================
    val meanAndStd = new MeanAndStdClassifier(reqCount, effectSizeThresholds, metricConfig)

    val resultBuilder = CanaryAnalysisResult.builder()
      .name(metric.getName)
      .id(metric.getId)
      .tags(metric.getTags)
      .groups(metricConfig.getGroups)
      .muted(isMutedMetric)
      .experimentMetadata(Map("stats" -> experimentStats.toMap.asJava.asInstanceOf[Object]).asJava)
      .controlMetadata(Map("stats" -> controlStats.toMap.asJava.asInstanceOf[Object]).asJava)

    try {
      val metricClassification = meanAndStd.classify(transformedControl, transformedExperiment, directionality, nanStrategy, isCriticalMetric, isDataRequired)
      resultBuilder
        .classification(metricClassification.classification.toString)
        .classificationReason(metricClassification.reason.orNull)
        .critical(metricClassification.critical)
        .resultMetadata(Map("ratio" -> metricClassification.deviation.asInstanceOf[Object]).asJava)
        .build()

    } catch {
      case e: RuntimeException =>
        logger.error("Metric Classification Failed", e)
        resultBuilder
          .critical(true)
          .classification(Error.toString)
          .classificationReason("Metric Classification Failed")
          .build()
    }
  }
}
