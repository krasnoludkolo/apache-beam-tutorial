package org.apache.beam.examples;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.PriorityQueue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;

final class MostSender extends PTransform<PCollection<Metric>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<Metric> input) {

        return input
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(Metric::getUser))
                .apply(Count.perElement())
                .apply(Combine.globally(new BestN(1)))
                .apply(MapElements.via(new FormatAsTextFn()));
    }

    class FormatAsTextFn extends SimpleFunction<List<KV<String, Long>>, String> {
        @Override
        public String apply(List<KV<String, Long>> input) {
            return input.toString();
        }
    }

    class BestN extends Combine.CombineFn<KV<String, Long>, PriorityQueue<BestN.Best>, List<KV<String, Long>>> {

        int n;

        BestN(int n) {
            this.n = n;
        }

        class Best {
            long bestValue = 0;
            List<KV<String, Long>> list = List.empty();

            Best(KV<String, Long> input) {
                this.bestValue = input.getValue();
                this.list = list.append(input);
            }

            Best(List<KV<String, Long>> list) {
                this.list = list;
                this.bestValue = list.get(0).getValue();
            }

            List<KV<String, Long>> getList() {
                return list;
            }
        }


        @Override
        public PriorityQueue<Best> createAccumulator() {
            return PriorityQueue.of(Comparator.comparingLong(b -> b.bestValue));
        }

        @Override
        public PriorityQueue<Best> addInput(PriorityQueue<Best> accumulator, KV<String, Long> input) {
            if (accumulator.peek().bestValue == input.getValue()) {
                List<KV<String, Long>> list = accumulator.peek().list.append(input);
                return accumulator.drop(1).enqueue(new Best(list));
            }else{
                return accumulator.enqueue(new Best(input)).take(n);
            }
        }

        @Override
        public PriorityQueue<Best> mergeAccumulators(Iterable<PriorityQueue<Best>> accumulators) {
          return List
                    .ofAll(accumulators)
                    .flatMap(queue -> queue)
                    .groupBy(best -> best.bestValue)
                    .toPriorityQueue(Comparator.comparingLong(t -> t._1))
                    .take(n)
                    .flatMap(t -> t._2);
        }

        @Override
        public List<KV<String, Long>> extractOutput(PriorityQueue<Best> accumulator) {
            return accumulator
                    .map(Best::getList)
                    .flatMap(l->l)
                    .toList();
        }

    }


}