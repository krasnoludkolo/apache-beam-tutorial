package analyser;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;

final class CreateWindowed<V> extends PTransform<PCollection<V>, PCollection<V>> implements Serializable {

    private SerializableFunction<V, Instant> createTimeStamp;
    private int windowDurationInSeconds;

    CreateWindowed(SerializableFunction<V, Instant> createTimeStamp, int windowDurationInSeconds) {
        this.createTimeStamp = createTimeStamp;
        this.windowDurationInSeconds = windowDurationInSeconds;
    }


    @Override
    public PCollection<V> expand(PCollection<V> input) {
        return input
                .apply("Timestamps", WithTimestamps.of(createTimeStamp))
                .apply("Window", Window
                        .into(FixedWindows.of(Duration.standardSeconds(windowDurationInSeconds)))
                );
    }
}