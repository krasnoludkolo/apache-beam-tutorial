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

public class MostSenderTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void emptyListInEmptyWindow() {
        TestStream<Metric> testData = TestStream.create(AvroCoder.of(Metric.class))
                .advanceWatermarkToInfinity();

        PCollection<List<List<KV<String, Long>>>> result = p
                .apply(testData)
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
                .apply(new MostSender(1));

        PAssert.that(result)
                .containsInAnyOrder(List.empty());

        p.run();
    }

    @Test
    public void oneMostSenderInOneWindow() {
        TestStream<Metric> testData = TestStream.create(AvroCoder.of(Metric.class))
                .addElements(
                        metric(123L, "a"),
                        metric(124L, "a"),
                        metric(125L, "b")
                )
                .advanceWatermarkToInfinity();

        PCollection<List<List<KV<String, Long>>>> result = p
                .apply(testData)
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
                .apply(new MostSender(1));

        PAssert.that(result)
                .containsInAnyOrder(List.of(List.of(KV.of("a", 2L))));

        p.run();
    }


    @Test
    public void manyMostSendersInOneWindow() {
        TestStream<Metric> testData = TestStream.create(AvroCoder.of(Metric.class))
                .addElements(
                        metric(123L, "a"),
                        metric(124L, "a"),
                        metric(125L, "b"),
                        metric(125L, "b"),
                        metric(125L, "c")
                )
                .advanceWatermarkToInfinity();

        PCollection<List<List<KV<String, Long>>>> result = p
                .apply(testData)
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
                .apply(new MostSender(1));

        PAssert.that(result)
                .containsInAnyOrder(List.of(List.of(KV.of("a", 2L), KV.of("b", 2L))));

        p.run();
    }

    private Metric metric(long timestamp, String name) {
        return new Metric("speed", timestamp, new long[]{}, name);
    }

}