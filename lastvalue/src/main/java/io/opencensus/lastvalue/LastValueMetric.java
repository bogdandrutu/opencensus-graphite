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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.export.Metric;
import io.opencensus.metrics.export.MetricDescriptor;
import io.opencensus.metrics.export.MetricDescriptor.Type;
import io.opencensus.metrics.export.Point;
import io.opencensus.metrics.export.TimeSeries;
import io.opencensus.metrics.export.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helper class that keeps only the last recorded value for each TimeSeries. This can be used when a
 * stream of already aggregated metrics is received (usually with a higher frequency than the export
 * interval).
 */
public final class LastValueMetric {
  /** Used when a value is not present (set) for a specific key. */
  public static final LabelValue UNSET_VALUE = LabelValue.create(null);

  private final MetricDescriptor metricDescriptor;
  private volatile ImmutableMap<ImmutableList<LabelValue>, MutableTimeSeries> registeredPoints =
      ImmutableMap.of();
  private final ImmutableList<LabelKey> labelKeys;
  private final boolean isCumulative;
  private final boolean isDouble;

  LastValueMetric(
      String name, String description, String unit, Type type, List<LabelKey> labelKeys) {
    // Not using copyOf because that may cause a full copy.
    this.labelKeys = ImmutableList.<LabelKey>builder().addAll(labelKeys).build();
    this.metricDescriptor = MetricDescriptor.create(name, description, unit, type, labelKeys);
    this.isCumulative = type == Type.CUMULATIVE_DOUBLE || type == Type.CUMULATIVE_INT64;
    this.isDouble = type == Type.CUMULATIVE_DOUBLE || type == Type.GAUGE_DOUBLE;
  }

  /**
   * Records a new value for one TimeSeries in this Metric. This is a more convenient way to record
   * because of the labels map, but comes with a cost of creating the list of labels inside this
   * method.
   *
   * @param labels the map of labels.
   * @param timestamp the timestamp when the value was captured.
   * @param value the captured value.
   */
  public void recordWithMapLabels(Map<String, String> labels, Timestamp timestamp, double value) {
    checkNotNull(labels, "labels");
    ImmutableList.Builder<LabelValue> builder =
        ImmutableList.builderWithExpectedSize(labelKeys.size());
    for (LabelKey labelKey : labelKeys) {
      String labelValueStr = labels.get(labelKey.getKey());
      if (labelValueStr == null) {
        builder.add(UNSET_VALUE);
        continue;
      }
      builder.add(LabelValue.create(labelValueStr));
    }
    record(builder.build(), timestamp, value);
  }

  /**
   * Records a new value for one TimeSeries in this Metric.
   *
   * @param labelValues the list of label values, the size of the list must be the same as the size
   *     of labelKeys in the constructor. If for a key no value available use {@link #UNSET_VALUE}.
   * @param timestamp the timestamp when the value was captured.
   * @param value the captured value.
   * @throws IllegalArgumentException if labelValues.size() != labelKeys.size().
   */
  public void record(ImmutableList<LabelValue> labelValues, Timestamp timestamp, double value) {
    checkNotNull(labelValues, "labelValues");
    checkNotNull(timestamp, "timestamp");
    // Safe to access the map without a lock because the map is immutable and volatile (so the
    // last written value is visible).
    MutableTimeSeries mutableTimeSeries = registeredPoints.get(labelValues);
    if (mutableTimeSeries != null) {
      // Fast path we already have the point
      mutableTimeSeries.set(timestamp, value);
      return;
    }

    // Slow path we need to add a new Point.
    checkArgument(
        labelKeys.size() == labelValues.size(),
        "Label Keys and Label Values don't have same size.");
    addMutablePoint(
        labelValues,
        isDouble
            ? new MutableDoubleTimeSeries(labelValues, timestamp, value, isCumulative)
            : new MutableLongTimeSeries(labelValues, timestamp, value, isCumulative));
  }

  // Synchronized here to make sure that two threads do not add a Point in the same time.
  private synchronized void addMutablePoint(
      ImmutableList<LabelValue> labelValues, MutableTimeSeries mutableTimeSeries) {
    // Synchronized here to make sure that two threads do not add a Point in the same time.
    registeredPoints =
        ImmutableMap.<ImmutableList<LabelValue>, MutableTimeSeries>builder()
            .putAll(registeredPoints)
            .put(labelValues, mutableTimeSeries)
            .build();
  }

  @Nullable
  Metric getMetric() {
    // Safe to access the map without a lock because the map is immutable and volatile (so the
    // last written value is visible).
    ImmutableMap<ImmutableList<LabelValue>, MutableTimeSeries> currentRegisteredPoints =
        registeredPoints;
    if (currentRegisteredPoints.isEmpty()) {
      return null;
    }

    List<TimeSeries> timeSeriesList = new ArrayList<>(currentRegisteredPoints.size());
    for (Map.Entry<ImmutableList<LabelValue>, MutableTimeSeries> entry :
        currentRegisteredPoints.entrySet()) {
      TimeSeries timeSeries = entry.getValue().getTimeSeries();
      if (timeSeries != null) {
        timeSeriesList.add(timeSeries);
      }
    }
    if (timeSeriesList.isEmpty()) {
      // All TimeSeries were empty so nothing to report.
      return null;
    }
    return Metric.create(metricDescriptor, timeSeriesList);
  }

  // When https://github.com/census-instrumentation/opencensus-java/issues/1789 is fixed
  // pre-create the default TimeSeries and use setPoint.
  private abstract static class MutableTimeSeries {
    private final ImmutableList<LabelValue> labelValues;
    private final boolean isCumulative;
    private Timestamp lastResetTimestamp;
    private double lastResetValue;
    // TODO: As an optimization to avoid locking can put this into an immutable class and use
    // volatile reference to that class here because load/store operations are atomic for
    // references.
    private Timestamp timestamp;
    private double value;

    MutableTimeSeries(
        ImmutableList<LabelValue> labelValues,
        Timestamp timestamp,
        double value,
        boolean isCumulative) {
      this.isCumulative = isCumulative;
      this.labelValues = labelValues;
      this.lastResetTimestamp = timestamp;
      this.lastResetValue = value;
      this.timestamp = timestamp;
      this.value = value;
    }

    synchronized void set(Timestamp timestamp, double value) {
      if (value < this.value) {
        // The value reset, we need to reset as well.
        this.lastResetTimestamp = timestamp;
        this.lastResetValue = value;
      }
      this.value = value;
      this.timestamp = timestamp;
    }

    @Nullable
    synchronized TimeSeries getTimeSeries() {
      if (isCumulative) {
        // If the last recorded point is the reset point, don't report anything because
        if (lastResetTimestamp.equals(timestamp)) {
          return null;
        }
        // We subtract the first recorded value and we always export relative to the first
        // recorded point.
        return TimeSeries.create(
            labelValues,
            Collections.singletonList(Point.create(getValue(value - lastResetValue), timestamp)),
            lastResetTimestamp);
      } else {
        return TimeSeries.create(
            labelValues, Collections.singletonList(Point.create(getValue(value), timestamp)), null);
      }
    }

    abstract Value getValue(double value);
  }

  private static final class MutableDoubleTimeSeries extends MutableTimeSeries {
    MutableDoubleTimeSeries(
        ImmutableList<LabelValue> labelValues,
        Timestamp timestamp,
        double value,
        boolean isCumulative) {
      super(labelValues, timestamp, value, isCumulative);
    }

    @Override
    Value getValue(double value) {
      return Value.doubleValue(value);
    }
  }

  private static final class MutableLongTimeSeries extends MutableTimeSeries {
    MutableLongTimeSeries(
        ImmutableList<LabelValue> labelValues,
        Timestamp timestamp,
        double value,
        boolean isCumulative) {
      super(labelValues, timestamp, value, isCumulative);
    }

    @Override
    Value getValue(double value) {
      // If we record only long values using round will give us the right long number because of
      // the floating point errors 0.999 means 1.
      return Value.longValue(Math.round(value));
    }
  }
}
