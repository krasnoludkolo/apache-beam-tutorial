package analyser;

import datasource.MetricDataSource;
import io.vavr.collection.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

import java.io.Serializable;

public final class MetricsAnalyzer implements Serializable {


    public void runAnalyse(PipelineOptions options, TestStream<Metric> stream) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(Read.from(new MetricDataSource()))
                .apply(new CreateWindowed<>(m -> new Instant(m.getTimestamp()), 1))
//                .apply(new MostSender(8))
//                .apply(MapElements
//                        .into(TypeDescriptors.strings())
//                        .via(Metric::getUser))
//                .apply(Count.perElement())
                .apply("MapToText", MapElements.via(new FormatAsTextFn()))
                .apply("FooterAndHeader", Combine.globally(new AddHeaderAndFooter("dupa","postDUpa")).withoutDefaults())
                .apply("MapToText", MapElements.via(new FormatAsTextFn()))
                .apply("Write", new WriteToFile());


        pipeline
                .run()
                .waitUntilFinish();
    }

//    public class FormatAsTextFn extends SimpleFunction<List<List<KV<String, Long>>>, String> {
//        @Override
//        public String apply(List<List<KV<String, Long>>> input) {
//            return input.toString();
//        }
//    }
    public class FormatAsTextFn extends SimpleFunction<Object, String> {
        @Override
        public String apply(Object input) {
            return input.toString();
        }
    }


    class WriteToFile extends PTransform<PCollection<String>, PDone> {
        @Override
        public PDone expand(PCollection<String> input) {

            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible("result/res");

            return input
                    .apply(TextIO.write().to(new PerWindowFiles(resource))
                            .withTempDirectory(resource.getCurrentDirectory())
                            .withWindowedWrites()
                            .withNumShards(10));
        }
    }


}
