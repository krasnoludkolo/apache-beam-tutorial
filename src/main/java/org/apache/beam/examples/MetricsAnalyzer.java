package org.apache.beam.examples;

import io.vavr.collection.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

import java.io.Serializable;

final class MetricsAnalyzer implements Serializable {


    void runAnalyse(PipelineOptions options, TestStream<Metric> stream) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(stream)
//                .apply("Timestamps", WithTimestamps.of((Metric m) -> new Instant(m.getTimestamp())))
//                .apply("Window", Window
//                        .<Metric>into(FixedWindows.of(Duration.standardSeconds(1)))
//                )
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
                .apply(new MostSender())
                .apply("MapToText", MapElements.via(new FormatAsTextFn()))
//                .apply(new HighestMetric("speed",3))
                .apply("Write", new WriteToFile());
        pipeline.run().waitUntilFinish();
    }

    class FormatAsTextFn extends SimpleFunction<List<List<KV<String, Long>>>, String> {
        @Override
        public String apply(List<List<KV<String, Long>>> input) {
            return input.toString();
        }
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
