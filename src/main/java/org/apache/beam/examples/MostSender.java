package org.apache.beam.examples;

import com.google.gson.Gson;
import io.vavr.Tuple2;
import io.vavr.collection.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.Serializable;

final class MostSender extends PTransform<PCollection<Metric>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<Metric> input) {

        return input
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(Metric::getUser))
                .apply(Count.perElement())
                .apply(Combine.globally(new BestN(2)).withoutDefaults())
                .apply(MapElements.via(new FormatAsTextFn()));
    }

    class FormatAsTextFn extends SimpleFunction<List<List<KV<String, Long>>>, String> {
        @Override
        public String apply(List<List<KV<String, Long>>> input) {
            return input.toString();
        }
    }

    class BestN extends Combine.CombineFn<KV<String, Long>, Map<Long,List<KV<String,Long>>>, List<List<KV<String, Long>>>> implements Serializable {

        private int n;

        BestN(int n) {
            this.n = n;
        }

        @Override
        public Map<Long, List<KV<String, Long>>> createAccumulator() {
            return TreeMap.empty();
        }

        @Override
        public Map<Long, List<KV<String, Long>>> addInput(Map<Long, List<KV<String, Long>>> accumulator, KV<String, Long> input) {
            return accumulator.put(input.getValue(),List.of(input),List::appendAll);
        }

        @Override
        public Map<Long, List<KV<String, Long>>> mergeAccumulators(Iterable<Map<Long, List<KV<String, Long>>>> accumulators) {
            return Stream.ofAll(accumulators).foldLeft(TreeMap.empty(),(m1, m2)->m1.merge(m2,List::appendAll));
        }

        @Override
        public List<List<KV<String, Long>>> extractOutput(Map<Long, List<KV<String, Long>>> accumulator) {
            return accumulator.takeRight(n).toList().map(l->l._2).reverse();
        }

    }
}