package datasource;

import analyser.Metric;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class MetricDataSource extends UnboundedSource<Metric, CustomCheckpointMark> {


    @Override
    public List<? extends UnboundedSource<Metric, CustomCheckpointMark>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        return Collections.singletonList(new MetricDataSource());
    }

    @Override
    public UnboundedReader<Metric> createReader(PipelineOptions options, @Nullable CustomCheckpointMark checkpointMark) throws IOException {
        return new MetricDataReader(this, checkpointMark);
    }

    @Override
    public Coder<CustomCheckpointMark> getCheckpointMarkCoder() {
        return SerializableCoder.of(CustomCheckpointMark.class);
    }

    @Override
    public Coder<Metric> getOutputCoder() {
        return AvroCoder.of(Metric.class);
    }
}
