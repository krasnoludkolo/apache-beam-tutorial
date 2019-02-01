package analyser;

import io.vavr.collection.List;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

final class HighestMetric extends PTransform<PCollection<Metric>, PCollection<List<List<KV<String, Long>>>>> {

    private String metricName;
    private int n;

    HighestMetric(String metricName, int n) {
        this.metricName = metricName;
        this.n = n;
    }

    @Override
    public PCollection<List<List<KV<String, Long>>>> expand(PCollection<Metric> input) {
        return input
                .apply(Filter.by((Metric m) -> m.getName().equals(metricName)))
                .apply(MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(),TypeDescriptors.longs()))
                        .via(m->KV.of(m.getUser(),m.getValues()[0]))
                )
                .apply(Combine.globally(new ListOfBestN(n)).withoutDefaults());
    }

}
