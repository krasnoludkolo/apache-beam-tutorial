package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;

final class MetricsAnalyzer implements Serializable {


    void runAnalyse(PipelineOptions options, TestStream<Metric> stream) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(stream)
                .apply("Timestamps", WithTimestamps.of((Metric m) -> new Instant(m.getTimestamp())))
                .apply("Window", Window
                        .<Metric>into(FixedWindows.of(Duration.standardSeconds(1)))
                )
                .apply(new MostSender())
//                .apply(new HighestMetric("speed",3))
                .apply("Write", new WriteToFile());
        pipeline.run().waitUntilFinish();
    }


    class WriteToFile extends PTransform<PCollection<String>, PDone> {
        @Override
        public PDone expand(PCollection<String> input) {

            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible("result");

            return input
                    .apply(TextIO.write().to(new PerWindowFiles(resource))
                            .withTempDirectory(resource.getCurrentDirectory())
                            .withWindowedWrites()
                            .withNumShards(1));
        }
    }


}
