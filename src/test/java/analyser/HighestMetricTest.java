package analyser;

import io.vavr.collection.List;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class HighestMetricTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void emptyListInEmptyWindow() {
        TestStream<Metric> testData = TestStream.create(AvroCoder.of(Metric.class))
                .advanceWatermarkToInfinity();

        PCollection<List<List<KV<String, Long>>>> result = p
                .apply(testData)
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
                    .apply(new HighestMetric("speed",3));

        PAssert.that(result)
                .containsInAnyOrder(List.empty());

        p.run();
    }

    @Test
    public void oneHighestMetricInOneWindow() {
        TestStream<Metric> testData = TestStream.create(AvroCoder.of(Metric.class))
                .addElements(
                        new Metric("speed",123L,new long[]{5},"a"),
                        new Metric("speed",123L,new long[]{1},"b")
                )
                .advanceWatermarkToInfinity();

        PCollection<List<List<KV<String, Long>>>> result = p
                .apply(testData)
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
                .apply(new HighestMetric("speed",1));

        PAssert.that(result)
                .containsInAnyOrder(List.of(List.of(KV.of("a", 5L))));

        p.run();
    }


    @Test
    public void manyHighestMetricsInOneWindow() {
        TestStream<Metric> testData = TestStream.create(AvroCoder.of(Metric.class))
                .addElements(
                        new Metric("speed",123L,new long[]{5},"a"),
                        new Metric("speed",123L,new long[]{2},"c"),
                        new Metric("speed",124L,new long[]{5},"b")
                )
                .advanceWatermarkToInfinity();

        PCollection<List<List<KV<String, Long>>>> result = p
                .apply(testData)
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
                .apply(new HighestMetric("speed",1));

        PAssert.that(result)
                .containsInAnyOrder(List.of(List.of(KV.of("a", 5L), KV.of("b", 5L))));

        p.run();
    }


}