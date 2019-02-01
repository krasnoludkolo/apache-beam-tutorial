package datasource;

import analyser.Metric;
import org.apache.beam.sdk.io.UnboundedSource;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class MetricDataReader extends UnboundedSource.UnboundedReader<Metric> {

    private Queue<Metric> queue = new ArrayDeque<>();
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private RandomMetricCreator randomMetric = new RandomMetricCreator();
    private MetricDataSource metricDataSource;
    private UnboundedSource.CheckpointMark checkpointMart;

    MetricDataReader(MetricDataSource metricDataSource, UnboundedSource.CheckpointMark checkpointMart) {
        this.metricDataSource = metricDataSource;
        this.checkpointMart = checkpointMart;
    }

    @Override
    public boolean start() throws IOException {
        scheduler.scheduleAtFixedRate(this::createAndAddRandomMetric, 0, 1, TimeUnit.MILLISECONDS);
        return advance();
    }

    private void createAndAddRandomMetric() {
        queue.add(randomMetric.create(Instant.now().getMillis()));
    }

    @Override
    public boolean advance() throws IOException {
        queue.poll();
        return queue.peek()!=null;
    }

    @Override
    public Metric getCurrent() throws NoSuchElementException {
        return queue.peek();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(queue.element().getTimestamp());
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdown();
    }

    @Override
    public Instant getWatermark() {
        return Instant.now();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return checkpointMart;
    }

    @Override
    public UnboundedSource<Metric, ?> getCurrentSource() {
        return metricDataSource;
    }
}
