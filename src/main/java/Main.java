import analyser.Metric;
import analyser.MetricsAnalyzer;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class Main {

    public static void main(String[] args) {
        TestStream<Metric> metrics = TestStream.create(AvroCoder.of(Metric.class))
                .addElements(
                        new Metric("speed", 123L, new long[]{3}, "a"),
                        new Metric("speed", 124L, new long[]{2}, "a"),
                        new Metric("speed", 125L, new long[]{1}, "b"),
                        new Metric("speed", 126L, new long[]{5}, "b"),
                        new Metric("speed", 127L, new long[]{4}, "c"),
                        new Metric("not speed", 128L, new long[]{1, 2}, "a"),
                        new Metric("not speed", 129L, new long[]{5, 2}, "b"),
                        new Metric("not speed", 129L, new long[]{5, 2}, "f"),
                        new Metric("not speed", 129L, new long[]{5, 2}, "f"),
                        new Metric("not speed", 133L, new long[]{3, 1}, "c")
                )
                .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(1)))
                .addElements(
                        new Metric("speed", 1123L, new long[]{3}, "d"),
                        new Metric("speed", 1124L, new long[]{2}, "d"),
                        new Metric("speed", 1125L, new long[]{1}, "b"),
                        new Metric("speed", 1126L, new long[]{5}, "e"),
                        new Metric("speed", 1127L, new long[]{4}, "a"),
                        new Metric("not speed", 1128L, new long[]{1, 2}, "a"),
                        new Metric("not speed", 1129L, new long[]{5, 2}, "b"),
                        new Metric("not speed", 1133L, new long[]{3, 1}, "x")
                )
                .advanceWatermarkTo(new Instant(0).plus(Duration.standardSeconds(2)))
                .addElements(
                        new Metric("speed", 2123L, new long[]{3}, "c"),
                        new Metric("speed", 2124L, new long[]{2}, "b"),
                        new Metric("speed", 2125L, new long[]{1}, "a"),
                        new Metric("speed", 2126L, new long[]{5}, "f"),
                        new Metric("speed", 2127L, new long[]{4}, "g"),
                        new Metric("not speed", 2128L, new long[]{1, 2}, "a"),
                        new Metric("not speed", 2129L, new long[]{5, 2}, "b"),
                        new Metric("not speed", 2133L, new long[]{3, 1}, "c")
                )
                .advanceWatermarkToInfinity();


        PipelineOptions options = PipelineOptionsFactory.create();
        new MetricsAnalyzer().runAnalyse(options, metrics);
    }

}
