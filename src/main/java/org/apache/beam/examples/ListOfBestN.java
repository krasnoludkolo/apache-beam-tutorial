package org.apache.beam.examples;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.collection.TreeMap;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

class ListOfBestN extends Combine.CombineFn<KV<String, Long>, Map<Long, List<KV<String, Long>>>, List<List<KV<String, Long>>>> implements Serializable {

    private int n;

    ListOfBestN(int n) {
        this.n = n;
    }

    @Override
    public Map<Long, List<KV<String, Long>>> createAccumulator() {
        return TreeMap.empty();
    }

    @Override
    public Map<Long, List<KV<String, Long>>> addInput(Map<Long, List<KV<String, Long>>> accumulator, KV<String, Long> input) {
        return accumulator.put(input.getValue(), List.of(input), List::appendAll);
    }

    @Override
    public Map<Long, List<KV<String, Long>>> mergeAccumulators(Iterable<Map<Long, List<KV<String, Long>>>> accumulators) {
        return Stream.ofAll(accumulators).foldLeft(TreeMap.empty(), (m1, m2) -> m1.merge(m2, List::appendAll));
    }

    @Override
    public List<List<KV<String, Long>>> extractOutput(Map<Long, List<KV<String, Long>>> accumulator) {
        return accumulator.takeRight(n).toList().map(l -> l._2).reverse();
    }

}
