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

package io.opencensus.graphite;

import com.google.common.collect.ImmutableList;
import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.export.ExportComponent;
import io.opencensus.metrics.export.Metric;
import io.opencensus.metrics.export.MetricDescriptor.Type;
import io.opencensus.metrics.export.MetricProducer;
import io.opencensus.metrics.export.MetricProducerManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

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
   * @param type the type of the metric (Gauge, Cumulative).
   * @param labelKeys the list of the label keys.
   * @param startTimestamp only for Cumulative metrics, and represents since when the metric is
   *     recorded.
   * @return
   */
  public synchronized LastValueMetric addMetric(
      String name,
      String description,
      String unit,
      Type type,
      List<LabelKey> labelKeys,
      @Nullable Timestamp startTimestamp) {
    LastValueMetric lastValueMetric =
        new LastValueMetric(name, description, unit, type, labelKeys, startTimestamp);
    List<LastValueMetric> copy = new ArrayList<>(lastValueMetrics);
    copy.add(lastValueMetric);
    return lastValueMetric;
  }

  @Override
  public Collection<Metric> getMetrics() {
    ImmutableList<LastValueMetric> currentLastValueMetrics = lastValueMetrics;
    ArrayList<Metric> ret = new ArrayList<>(currentLastValueMetrics.size());
    for (LastValueMetric lastValueMetric : currentLastValueMetrics) {
      ret.add(lastValueMetric.getMetric());
    }
    return ret;
  }
}
