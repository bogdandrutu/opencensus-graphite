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

package io.opencensus.example;

import com.google.api.MonitoredResource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.opencensus.common.Duration;
import io.opencensus.common.Timestamp;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.lastvalue.LastValueMetric;
import io.opencensus.lastvalue.LastValueProducer;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.Metrics;
import io.opencensus.metrics.export.MetricDescriptor.Type;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public final class SendMetricsToStackdriver {
  private static final Logger logger = Logger.getLogger(SendMetricsToStackdriver.class.getName());
  private static final Random random = new Random(1234);

  private static final String CLOUD_GUID = "cloud_guid";
  private static final String TYPE = "type";

  private static void initExporter() throws IOException {
    Map<String, String> labels = new HashMap<>();
    labels.put("project_id", "e2e-debugging");
    labels.put("location", "us-central1-a");
    labels.put("namespace", "IAXNNFD7");
    labels.put("node_id", "ip-10-10-10-10");
    com.google.api.MonitoredResource resource =
        MonitoredResource.newBuilder().setType("generic_node").putAllLabels(labels).build();

    StackdriverStatsConfiguration.Builder statsConfigurationBuilder =
        StackdriverStatsConfiguration.builder()
            .setMonitoredResource(resource)
            .setExportInterval(Duration.create(TimeUnit.SECONDS.toMillis(30), 0))
            .setProjectId("e2e-debugging")
            .setMetricNamePrefix("custom.googleapis.com/lastvalue");

    StackdriverStatsConfiguration statsConfiguration = statsConfigurationBuilder.build();

    logger.info("Register metric exporter with resource " + resource.toString());
    StackdriverStatsExporter.createAndRegister(statsConfiguration);
  }

  private static void sendMetrics(LastValueProducer lastValueProducer) throws InterruptedException {
    LastValueMetric lastValueMetric =
        lastValueProducer.addMetric(
            "total_bytes",
            "My test metric.",
            "By",
            Type.CUMULATIVE_DOUBLE,
            ImmutableList.of(LabelKey.create(CLOUD_GUID, ""), LabelKey.create(TYPE, "")));
    ImmutableMap<String, String> labels =
        ImmutableMap.of(CLOUD_GUID, "80aa91e7-0583-4404-9c83-dc7acd7bca56", TYPE, "read");

    // Now send values every 8 seconds. We increase the value we send for 13 times then reset.
    double value = 0.0;
    for (int i = 0; i < 1000; i++) {
      if (i % 13 == 0) {
        // Reset point
        value = randomValue(0.0);
      } else {
        value = randomValue(value);
      }
      lastValueMetric.recordWithMapLabels(
          labels, Timestamp.fromMillis(System.currentTimeMillis()), value);
      Thread.sleep(TimeUnit.SECONDS.toMillis(30));
    }
  }

  private static double randomValue(double previous) {
    return 13.2 + previous + random.nextDouble() * 10;
  }

  /** Main function. */
  public static void main(String... args) throws InterruptedException {
    LastValueProducer lastValueProducer = new LastValueProducer();
    Metrics.getExportComponent().getMetricProducerManager().add(lastValueProducer);
    try {
      initExporter();
    } catch (IOException e) {
      logger.warning("Failed to create and register OpenCensus Prometheus Stats exporter " + e);
    }

    sendMetrics(lastValueProducer);

    Thread.sleep(TimeUnit.SECONDS.toMillis(60));
  }

  private SendMetricsToStackdriver() {}
}
