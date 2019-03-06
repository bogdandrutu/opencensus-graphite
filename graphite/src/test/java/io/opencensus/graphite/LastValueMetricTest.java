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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import io.opencensus.common.Duration;
import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.export.MetricDescriptor;
import io.opencensus.metrics.export.MetricDescriptor.Type;
import io.opencensus.metrics.export.Point;
import io.opencensus.metrics.export.TimeSeries;
import io.opencensus.metrics.export.Value;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LastValueMetric}. */
@RunWith(JUnit4.class)
public class LastValueMetricTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String METRIC_NAME = "name";
  private static final String METRIC_DESCRIPTION = "description";
  private static final String METRIC_UNIT = "1";
  private static final ImmutableList<LabelKey> LABEL_KEY =
      ImmutableList.of(LabelKey.create("key", "key description"));
  private static final ImmutableList<LabelValue> LABEL_VALUES =
      ImmutableList.of(LabelValue.create("value"));
  private static final ImmutableList<LabelValue> LABEL_VALUES1 =
      ImmutableList.of(LabelValue.create("value1"));

  private static final Timestamp TEST_TIME = Timestamp.create(1234, 123);
  private static final Timestamp TEST_TIME1 = TEST_TIME.addDuration(Duration.create(1, 1));
  private static final Timestamp TEST_TIME2 = TEST_TIME1.addDuration(Duration.create(1, 1));
  private static final MetricDescriptor GAUGE_METRIC_DESCRIPTOR =
      MetricDescriptor.create(
          METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.GAUGE_DOUBLE, LABEL_KEY);
  private static final MetricDescriptor CUMULATIVE_METRIC_DESCRIPTOR =
      MetricDescriptor.create(
          METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.CUMULATIVE_DOUBLE, LABEL_KEY);
  private final LastValueMetric gaugeMetric =
      new LastValueMetric(
          METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.GAUGE_DOUBLE, LABEL_KEY);
  private final LastValueMetric cumulativeMetric =
      new LastValueMetric(
          METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.CUMULATIVE_DOUBLE, LABEL_KEY);

  @Test
  public void record_WithNullLabelValues() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("labelValues");
    gaugeMetric.record(null, TEST_TIME, 5);
  }

  @Test
  public void record_WithNullTimestamp() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("timestamp");
    gaugeMetric.record(LABEL_VALUES, null, 5);
  }

  @Test
  public void getMetric_Gauge() {
    assertThat(gaugeMetric.getMetric()).isNull();
    gaugeMetric.record(LABEL_VALUES, TEST_TIME, 5);
    assertThat(gaugeMetric.getMetric()).isNotNull();
    assertThat(gaugeMetric.getMetric().getMetricDescriptor()).isEqualTo(GAUGE_METRIC_DESCRIPTOR);
    assertThat(gaugeMetric.getMetric().getTimeSeriesList())
        .containsExactly(
            ImmutableList.of(
                TimeSeries.create(
                    LABEL_VALUES,
                    ImmutableList.of(Point.create(Value.doubleValue(5), TEST_TIME)),
                    null)));
    gaugeMetric.record(LABEL_VALUES, TEST_TIME1, 3);
    assertThat(gaugeMetric.getMetric().getTimeSeriesList())
        .containsExactly(
            ImmutableList.of(
                TimeSeries.create(
                    LABEL_VALUES,
                    ImmutableList.of(Point.create(Value.doubleValue(3), TEST_TIME1)),
                    null)));
    gaugeMetric.record(LABEL_VALUES, TEST_TIME2, 7);
    assertThat(gaugeMetric.getMetric().getTimeSeriesList())
        .containsExactly(
            ImmutableList.of(
                TimeSeries.create(
                    LABEL_VALUES,
                    ImmutableList.of(Point.create(Value.doubleValue(7), TEST_TIME1)),
                    null)));
  }

  @Test
  public void getMetric_Cumulative() {
    assertThat(cumulativeMetric.getMetric()).isNull();
    cumulativeMetric.record(LABEL_VALUES, TEST_TIME, 5);
    assertThat(cumulativeMetric.getMetric()).isNotNull();
    assertThat(cumulativeMetric.getMetric().getMetricDescriptor())
        .isEqualTo(CUMULATIVE_METRIC_DESCRIPTOR);
    // First recorded value
    assertThat(cumulativeMetric.getMetric().getTimeSeriesList())
        .containsExactly(
            ImmutableList.of(
                TimeSeries.create(
                    LABEL_VALUES,
                    ImmutableList.of(Point.create(Value.doubleValue(0), TEST_TIME)),
                    TEST_TIME)));
    cumulativeMetric.record(LABEL_VALUES, TEST_TIME1, 12);
    // Second record, export the difference.
    assertThat(cumulativeMetric.getMetric().getTimeSeriesList())
        .containsExactly(
            ImmutableList.of(
                TimeSeries.create(
                    LABEL_VALUES,
                    ImmutableList.of(Point.create(Value.doubleValue(7), TEST_TIME1)),
                    TEST_TIME)));
    // Newly added TimeSeries also starts from 0.
    cumulativeMetric.record(LABEL_VALUES1, TEST_TIME1, 13);
    assertThat(cumulativeMetric.getMetric().getTimeSeriesList())
        .containsExactly(
            ImmutableList.of(
                TimeSeries.create(
                    LABEL_VALUES,
                    ImmutableList.of(Point.create(Value.doubleValue(7), TEST_TIME1)),
                    TEST_TIME),
                TimeSeries.create(
                    LABEL_VALUES1,
                    ImmutableList.of(Point.create(Value.doubleValue(0), TEST_TIME1)),
                    TEST_TIME1)));
    // Newly added TimeSeries also starts from 0.
    cumulativeMetric.record(LABEL_VALUES1, TEST_TIME2, 18);
    cumulativeMetric.record(LABEL_VALUES1, TEST_TIME2, 16);
    assertThat(cumulativeMetric.getMetric().getTimeSeriesList())
        .containsExactly(
            ImmutableList.of(
                TimeSeries.create(
                    LABEL_VALUES,
                    ImmutableList.of(Point.create(Value.doubleValue(6), TEST_TIME2)),
                    TEST_TIME),
                TimeSeries.create(
                    LABEL_VALUES1,
                    ImmutableList.of(Point.create(Value.doubleValue(3), TEST_TIME2)),
                    TEST_TIME1)));
  }
}
