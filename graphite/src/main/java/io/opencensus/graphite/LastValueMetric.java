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
  private volatile ImmutableMap<ImmutableList<LabelValue>, MutablePoint> registeredPoints =
      ImmutableMap.of();
  private final int labelKeysSize;

  LastValueMetric(
      String name, String description, String unit, Type type, List<LabelKey> labelKeys) {
    labelKeysSize = labelKeys.size();
    this.metricDescriptor = MetricDescriptor.create(name, description, unit, type, labelKeys);
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
    MutablePoint mutablePoint = registeredPoints.get(labelValues);
    if (mutablePoint != null) {
      // Fast path we already have the point
      mutablePoint.set(timestamp, value);
      return;
    }

    // Slow path we need to add a new Point.
    checkArgument(
        labelKeysSize == labelValues.size(), "Label Keys and Label Values don't have same size.");
    addMutablePoint(
        labelValues,
        new MutablePoint(
            labelValues, timestamp, value, metricDescriptor.getType() == Type.CUMULATIVE_DOUBLE));
  }

  // Synchronized here to make sure that two threads do not add a Point in the same time.
  private synchronized void addMutablePoint(
      ImmutableList<LabelValue> labelValues, MutablePoint mutablePoint) {
    // Synchronized here to make sure that two threads do not add a Point in the same time.
    registeredPoints =
        ImmutableMap.<ImmutableList<LabelValue>, MutablePoint>builder()
            .putAll(registeredPoints)
            .put(labelValues, mutablePoint)
            .build();
  }

  @Nullable
  Metric getMetric() {
    // Safe to access the map without a lock because the map is immutable and volatile (so the
    // last written value is visible).
    ImmutableMap<ImmutableList<LabelValue>, MutablePoint> currentRegisteredPoints =
        registeredPoints;
    if (currentRegisteredPoints.isEmpty()) {
      return null;
    }

    if (currentRegisteredPoints.size() == 1) {
      MutablePoint point = currentRegisteredPoints.values().iterator().next();
      return Metric.createWithOneTimeSeries(metricDescriptor, point.getTimeSeries());
    }

    List<TimeSeries> timeSeriesList = new ArrayList<>(currentRegisteredPoints.size());
    for (Map.Entry<ImmutableList<LabelValue>, MutablePoint> entry :
        currentRegisteredPoints.entrySet()) {
      timeSeriesList.add(entry.getValue().getTimeSeries());
    }
    return Metric.create(metricDescriptor, timeSeriesList);
  }

  // When is fixed pre-create the default TimeSeries and use setPoint.
  private static final class MutablePoint {
    private final ImmutableList<LabelValue> labelValues;
    private final boolean isCumulative;
    private Timestamp lastResetTimestamp;
    private double lastResetValue;
    // TODO: As an optimization to avoid locking can put this into an immutable class and use
    // volatile reference to that class here because load/store operations are atomic for
    // references.
    private Timestamp timestamp;
    private double value;

    MutablePoint(
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

    synchronized TimeSeries getTimeSeries() {
      if (isCumulative) {
        // We subtract the first recorded value and we always export relative to the first
        // recorded point.
        return TimeSeries.create(
            labelValues,
            Collections.singletonList(
                Point.create(Value.doubleValue(value - lastResetValue), timestamp)),
            lastResetTimestamp);
      } else {
        return TimeSeries.create(
            labelValues,
            Collections.singletonList(Point.create(Value.doubleValue(value), timestamp)),
            null);
      }
    }
  }
}
