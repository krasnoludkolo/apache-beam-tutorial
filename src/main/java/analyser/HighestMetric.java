package analyser;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

final class HighestMetric extends PTransform<PCollection<Metric>, PCollection<String>> {

    private String metricName;
    private int n;

    public HighestMetric(String metricName, int n) {
        this.metricName = metricName;
        this.n = n;
    }

    @Override
    public PCollection<String> expand(PCollection<Metric> input) {
        return input
                .apply(Filter.by((Metric m) -> m.getName().equals(metricName)))
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(Metric::getUser)
                )
                .apply(Count.perElement())
                .apply(MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
                        .via((KV<String, Long> kv) -> KV.of("", kv)))
                .apply(Top.perKey(n, (Comparator<KV<String, Long>> & Serializable)
                        (KV<String, Long> x, KV<String, Long> y) ->
                                x.getValue().compareTo(y.getValue())))
                .apply(MapElements.via(new FormatAsTextFn()));
    }

    class FormatAsTextFn extends SimpleFunction<KV<String, List<KV<String, Long>>>, String> {
        @Override
        public String apply(KV<String, List<KV<String, Long>>> input) {
            return input.getValue()
                    .stream()
                    .map((KV kv) -> "{" + kv.getKey() + ":" + kv.getValue() + "}")
                    .collect(Collectors.joining(","));
        }
    }

}
