package org.apache.beam.examples;

import io.vavr.collection.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

final class MostSender extends PTransform<PCollection<Metric>, PCollection<List<List<KV<String, Long>>>>> {

    @Override
    public PCollection<List<List<KV<String, Long>>>> expand(PCollection<Metric> input) {

        return input
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(Metric::getUser))
                .apply(Count.perElement())
                .apply("FindBest",Combine.globally(new ListOfBestN(1)).withoutDefaults());
    }


}