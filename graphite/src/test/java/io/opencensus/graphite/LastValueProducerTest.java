package io.opencensus.graphite;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.export.MetricDescriptor;
import io.opencensus.metrics.export.MetricDescriptor.Type;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LastValueProducer}. */
@RunWith(JUnit4.class)
public class LastValueProducerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String METRIC_NAME = "name";
  private static final String METRIC_DESCRIPTION = "description";
  private static final String METRIC_UNIT = "1";
  private static final ImmutableList<LabelKey> LABEL_KEY =
      ImmutableList.of(LabelKey.create("key", "key description"));
  private static final ImmutableList<LabelValue> LABEL_VALUES =
      ImmutableList.of(LabelValue.create("value"));

  private static final Timestamp TEST_TIME = Timestamp.create(1234, 123);
  private static final MetricDescriptor GAUGE_METRIC_DESCRIPTOR =
      MetricDescriptor.create(
          METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.GAUGE_DOUBLE, LABEL_KEY);
  private static final MetricDescriptor CUMULATIVE_METRIC_DESCRIPTOR =
      MetricDescriptor.create(
          METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.CUMULATIVE_DOUBLE, LABEL_KEY);
  private final LastValueProducer lastValueProducer = new LastValueProducer();

  @Test
  public void addMetrics_WithNullName() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("labelValues");
    lastValueProducer.addMetric(
        null, METRIC_DESCRIPTION, METRIC_UNIT, Type.GAUGE_DOUBLE, LABEL_KEY);
  }

  @Test
  public void addMetrics_WithNullDescription() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("descriptor");
    lastValueProducer.addMetric(METRIC_NAME, null, METRIC_UNIT, Type.GAUGE_DOUBLE, LABEL_KEY);
  }

  @Test
  public void addMetrics_WithNullUnit() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("unit");
    lastValueProducer.addMetric(
        METRIC_NAME, METRIC_DESCRIPTION, null, Type.GAUGE_DOUBLE, LABEL_KEY);
  }

  @Test
  public void addMetrics_WithNullType() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("type");
    lastValueProducer.addMetric(METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, null, LABEL_KEY);
  }

  @Test
  public void addMetrics_WithNullLabelKeys() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("labelKeys");
    lastValueProducer.addMetric(
        METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.GAUGE_DOUBLE, null);
  }

  @Test
  public void addMetrics_Gauge() {
    LastValueMetric metric =
        lastValueProducer.addMetric(
            METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.GAUGE_DOUBLE, LABEL_KEY);
    assertThat(metric).isNotNull();
    assertThat(metric.getMetric()).isNull();
    assertThat(lastValueProducer.getMetrics()).isEmpty();
    metric.record(LABEL_VALUES, TEST_TIME, 5);
    assertThat(metric.getMetric()).isNotNull();
    assertThat(metric.getMetric().getMetricDescriptor()).isEqualTo(GAUGE_METRIC_DESCRIPTOR);
    assertThat(lastValueProducer.getMetrics()).containsExactly(metric.getMetric());
  }

  @Test
  public void addMetrics_Cumulative() {
    LastValueMetric metric =
        lastValueProducer.addMetric(
            METRIC_NAME, METRIC_DESCRIPTION, METRIC_UNIT, Type.CUMULATIVE_DOUBLE, LABEL_KEY);
    assertThat(metric).isNotNull();
    assertThat(metric.getMetric()).isNull();
    assertThat(lastValueProducer.getMetrics()).isEmpty();
    metric.record(LABEL_VALUES, TEST_TIME, 5);
    assertThat(metric.getMetric()).isNotNull();
    assertThat(metric.getMetric().getMetricDescriptor()).isEqualTo(CUMULATIVE_METRIC_DESCRIPTOR);
    assertThat(lastValueProducer.getMetrics()).containsExactly(metric.getMetric());
  }
}
