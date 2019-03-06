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
    checkNotNull(timestamp, "timestamp");
    // Safe to access the map without a lock because the map is immutable and volatile (so the
    // last written value is visible).
    MutablePoint mutablePoint = registeredPoints.get(labelValues);
    if (mutablePoint != null) {
      // Fast path we already have the point
      mutablePoint.set(timestamp, value);
      return;
    }

    checkArgument(
        labelKeysSize == labelValues.size(), "Label Keys and Label Values don't have same size.");

    synchronized (this) {
      registeredPoints =
          ImmutableMap.<ImmutableList<LabelValue>, MutablePoint>builder()
              .putAll(registeredPoints)
              .put(
                  labelValues,
                  new MutablePoint(
                      labelValues,
                      timestamp,
                      value,
                      metricDescriptor.getType() == Type.CUMULATIVE_DOUBLE))
              .build();
    }
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

  private static final class MutablePoint {
    private final TimeSeries defaultTimeSeries;
    private final double firstRecordedValue;
    private final boolean isCumulative;
    // TODO: As an optimization to avoid locking can put this into an immutable class and use
    // volatile reference to that class here because load/store operations are atomic for
    // references.
    private Timestamp timestamp;
    private double value;

    MutablePoint(
        List<LabelValue> labelValues, Timestamp timestamp, double value, boolean isCumulative) {
      this.isCumulative = isCumulative;
      defaultTimeSeries =
          isCumulative
              ? TimeSeries.create(labelValues, Collections.emptyList(), timestamp)
              : TimeSeries.create(labelValues);
      firstRecordedValue = value;
      this.timestamp = timestamp;
      this.value = value;
    }

    synchronized void set(Timestamp timestamp, double value) {
      this.value = value;
      this.timestamp = timestamp;
    }

    synchronized TimeSeries getTimeSeries() {
      if (isCumulative) {
        // We subtract the first recorded value and we always export relative to the first
        // recorded point.
        return defaultTimeSeries.setPoint(
            Point.create(Value.doubleValue(value - firstRecordedValue), timestamp));
      } else {
        return defaultTimeSeries.setPoint(
            Point.create(Value.doubleValue(value - firstRecordedValue), timestamp));
      }
    }
  }
}
