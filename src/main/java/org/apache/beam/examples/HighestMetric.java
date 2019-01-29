package org.apache.beam.examples;

import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

final class HighestSpeed extends PTransform<PCollection<Metric>, PCollection<String>> {


    @Override
    public PCollection<String> expand(PCollection<Metric> input) {
        return input
                .apply(Filter.by((Metric m)->m.getName().equals("speed")));
    }
}
