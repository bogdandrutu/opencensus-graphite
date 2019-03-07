/*
 * Copyright 2019, OpenCensus Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opencensus.lastvalue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.opencensus.metrics.export.MetricDescriptor.Type.CUMULATIVE_DOUBLE;
import static io.opencensus.metrics.export.MetricDescriptor.Type.CUMULATIVE_INT64;
import static io.opencensus.metrics.export.MetricDescriptor.Type.GAUGE_DOUBLE;
import static io.opencensus.metrics.export.MetricDescriptor.Type.GAUGE_INT64;

import com.google.common.collect.ImmutableList;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.export.ExportComponent;
import io.opencensus.metrics.export.Metric;
import io.opencensus.metrics.export.MetricDescriptor.Type;
import io.opencensus.metrics.export.MetricProducer;
import io.opencensus.metrics.export.MetricProducerManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Metric producer that is designed for the cases when a stream of already aggregated metrics is
 * received from an external source (maybe more frequent than the exporting time).
 *
 * <p>This producer MUST be register to the manager {@link
 * ExportComponent#getMetricProducerManager()} using {@link
 * MetricProducerManager#add(MetricProducer)}.
 */
public final class LastValueProducer extends MetricProducer {
  ImmutableList<LastValueMetric> lastValueMetrics = ImmutableList.of();

  public LastValueProducer() {}

  /**
   * Creates a new {@link LastValueMetric} and adds it to the list of exported metrics.
   *
   * @param name the name of the metric.
   * @param description the description of the metric.
   * @param unit the unit of the metric.
   * @param type the type of the metric (Gauge, Cumulative), supported values are {@link
   *     Type#CUMULATIVE_DOUBLE}, {@link Type#GAUGE_DOUBLE}, {@link Type#CUMULATIVE_INT64} and
   *     {@link Type#GAUGE_INT64}.
   * @param labelKeys the list of the label keys.
   * @return a {@link LastValueMetric} that can be used to record TimeSeries for this metric.
   * @throws IllegalArgumentException if type is not {@link Type#CUMULATIVE_DOUBLE} or {@link
   *     Type#GAUGE_DOUBLE} or {@link Type#CUMULATIVE_INT64} or {@link Type#GAUGE_INT64}.
   */
  public synchronized LastValueMetric addMetric(
      String name, String description, String unit, Type type, List<LabelKey> labelKeys) {
    checkNotNull(name, "name");
    checkNotNull(description, "description");
    checkNotNull(unit, "unit");
    checkNotNull(type, "type");
    checkNotNull(labelKeys, "labelKeys");
    checkArgument(
        type == CUMULATIVE_DOUBLE
            || type == GAUGE_DOUBLE
            || type == CUMULATIVE_INT64
            || type == GAUGE_INT64,
        "Type must be " + "CUMULATIVE_DOUBLE or GAUGE_DOUBLE");
    LastValueMetric lastValueMetric = new LastValueMetric(name, description, unit, type, labelKeys);
    lastValueMetrics =
        ImmutableList.<LastValueMetric>builder()
            .addAll(lastValueMetrics)
            .add(lastValueMetric)
            .build();

    return lastValueMetric;
  }

  @Override
  public Collection<Metric> getMetrics() {
    ImmutableList<LastValueMetric> currentLastValueMetrics = lastValueMetrics;
    ArrayList<Metric> ret = new ArrayList<>(currentLastValueMetrics.size());
    for (LastValueMetric lastValueMetric : currentLastValueMetrics) {
      Metric metric = lastValueMetric.getMetric();
      if (metric != null) {
        ret.add(lastValueMetric.getMetric());
      }
    }
    return ret;
  }
}
