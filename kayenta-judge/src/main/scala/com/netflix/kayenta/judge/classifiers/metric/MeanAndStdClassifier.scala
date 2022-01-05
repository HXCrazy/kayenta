/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.kayenta.judge.classifiers.metric
import com.netflix.kayenta.canary.CanaryMetricConfig
import com.netflix.kayenta.judge.Metric
import com.netflix.kayenta.judge.stats.{DescriptiveStatistics, MetricStatistics}
import com.netflix.kayenta.judge.utils.MapUtils

case class MeanAndStdComparisonResult(classification: MetricClassificationLabel, reason: Option[String], score: Double, threhold: Double)

class MeanAndStdClassifier(reqCount:Double=Double.NaN,
                           effectSizeThresholds:(Double, Double)=(1.0, 1.0), metricConfig:CanaryMetricConfig) extends BaseMetricClassifier{

  override def classify(control: Metric, experiment: Metric, direction: MetricDirection,
                        nanStrategy: NaNStrategy, isCriticalMetric: Boolean, isDataRequired: Boolean): MetricClassification = {
    //Check if there is no-data for the experiment or control
    if (experiment.values.isEmpty || control.values.isEmpty) {
      val reason = s"Missing data for ${experiment.name}"
      return MetricClassification(Nodata, Some(reason), 0.0, isCriticalMetric)
    }
    val experimentStats = DescriptiveStatistics.summary(experiment)
    val controlStats = DescriptiveStatistics.summary(control)

    val allowJitter = MapUtils.getAsBooleanWithDefault(true, metricConfig.getAnalysisConfigurations, "canary", "allowJitter", "enable")
    val allowJitterThreshold = MapUtils.getAsDoubleWithDefault(Double.NaN, metricConfig.getAnalysisConfigurations, "canary", "allowJitter", "threshold")

    if (!allowJitter && !allowJitterThreshold.isNaN && !checkJitter(allowJitterThreshold, controlStats, experimentStats, direction)){
      val reason = if (controlStats.mean > experimentStats.mean) s"The metric ${experiment.name} was classified as $High (Critical)" else s"The metric ${experiment.name} was classified as $Low (Critical)"
      val classificationResult = if (controlStats.mean > experimentStats.mean)  High else Low
      return MetricClassification(classificationResult, Some(reason), 0.0, critical = true)
    }
    val comparisonResult = compare(control, experiment, controlStats, experimentStats, metricConfig)
    var value = 1.0
    // Eliminate trends that can be tolerated
    if (High.equals(comparisonResult.classification) || Low.equals(comparisonResult.classification)){
      value = 0.0
      if (direction == MetricDirection.Increase){
        if (controlStats.mean > experimentStats.mean && controlStats.std <= experimentStats.std){
          return MetricClassification(Pass, None, 1.0, critical = true)
        }
      }
      if (direction == MetricDirection.Decrease){
        if (controlStats.mean < experimentStats.mean && controlStats.std >= experimentStats.std){
          return MetricClassification(Pass, None, 1.0, critical = true)
        }
      }
    }
    // Bottom-up strategy, Compare average
    val bottomupStrategyString = MapUtils.getAsStringWithDefault("none", metricConfig.getAnalysisConfigurations, "canary", "bottomup", "strategy")
    val bottomupStrategyThreshold = MapUtils.getAsDoubleWithDefault(Double.NaN, metricConfig.getAnalysisConfigurations, "canary", "bottomup", "threshold")
    val bottomupStrategy = BottomupStrategy.parse(bottomupStrategyString)
    if (bottomupStrategy != BottomupStrategy.Nothing && !bottomupStrategyThreshold.isNaN){
      if (bottomupStrategy == BottomupStrategy.Less && experimentStats.mean > bottomupStrategyThreshold){
        MetricClassification(High, None, 0.0, critical = true)
      }
      if (bottomupStrategy == BottomupStrategy.Greater && experimentStats.mean < bottomupStrategyThreshold){
        MetricClassification(Low, None, 0.0, critical = true)
      }
    }
    MetricClassification(comparisonResult.classification, comparisonResult.reason,  value, critical = true)
  }

  private def checkJitter(allowJitterThreshold:Double, controlStats:MetricStatistics, experimentStats:MetricStatistics, direction: MetricDirection):Boolean={
    if (controlStats.std == 0 && controlStats.mean == allowJitterThreshold && experimentStats.std > 0) {
      if ((direction == MetricDirection.Decrease || direction == MetricDirection.Either) && controlStats.mean > experimentStats.mean){
        return false
      }
      if ((direction == MetricDirection.Increase || direction == MetricDirection.Either) && controlStats.mean < experimentStats.mean){
        return false
      }
    }
    true
  }

  private def compare(control: Metric, experiment: Metric, controlStats:MetricStatistics, experimentStats:MetricStatistics, metricConfig:CanaryMetricConfig):MeanAndStdComparisonResult={
    val numerator = new java.math.BigDecimal(math.abs(controlStats.mean - experimentStats.mean) + math.abs(controlStats.std - experimentStats.std))
    val denominator = new java.math.BigDecimal(controlStats.mean + controlStats.std + math.pow(10,-10))
    val baseScore = numerator.divide(denominator, 8, java.math.RoundingMode.HALF_UP).doubleValue()

    val caculateThreholdValue = caculateThrehold(metricConfig)
    if (caculateThreholdValue.isNaN){
      val reason = s"${experiment.name} was classified as $ThresholdError"
      return MeanAndStdComparisonResult(ThresholdError, Some(reason), baseScore, caculateThreholdValue)
    }
    if (baseScore <= caculateThreholdValue){
      MeanAndStdComparisonResult(Pass, None, baseScore, caculateThreholdValue)
    }else {
      if (controlStats.mean > experimentStats.mean){
        val reason = s"${experiment.name} was classified as $Low"
        MeanAndStdComparisonResult(Low, Some(reason), baseScore, caculateThreholdValue)
      }else{
        val reason = s"${experiment.name} was classified as $High"
        MeanAndStdComparisonResult(High, Some(reason), baseScore, caculateThreholdValue)
      }

    }
  }

  private def caculateThrehold(metricConfig:CanaryMetricConfig):Double={
    var threshold = Double.NaN
    if (reqCount.isNaN){
      // Change the value to a standard threshold
      threshold = metricConfig.getStandardThreshold
//      if (null == threshold || threshold <= 0){
//        threshold = 0.00001
//      }
    }else{
      val powerA = MapUtils.getAsDoubleWithDefault(Double.NaN, metricConfig.getAnalysisConfigurations, "canary", "powerA")
      val powerB = MapUtils.getAsDoubleWithDefault(Double.NaN, metricConfig.getAnalysisConfigurations, "canary", "powerB")
      if (!powerA.isNaN && !powerB.isNaN){
        threshold = powerA * math.pow(reqCount, powerB)
      }
    }
    threshold
  }
}
