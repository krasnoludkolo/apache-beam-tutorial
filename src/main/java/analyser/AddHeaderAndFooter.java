package analyser;

import io.vavr.collection.List;
import io.vavr.collection.Stream;
import org.apache.beam.sdk.transforms.Combine;

final class AddHeaderAndFooter extends Combine.CombineFn<String, List<String>, List<String>>  {

    private String header;
    private String footer;

    AddHeaderAndFooter(String header, String footer) {
        this.header = header;
        this.footer = footer;
    }


    @Override
    public List<String> createAccumulator() {
        return List.empty();
    }

    @Override
    public List<String> addInput(List<String> accumulator, String input) {
        return accumulator.append(input);
    }

    @Override
    public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
        return Stream.ofAll(accumulators).foldLeft(List.empty(),List::appendAll);
    }

    @Override
    public List<String> extractOutput(List<String> accumulator) {
        return List.of(header)
                .appendAll(accumulator)
                .append(footer);
    }
}
