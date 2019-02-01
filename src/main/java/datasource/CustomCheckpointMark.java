package datasource;

import org.apache.beam.sdk.io.UnboundedSource;

import java.io.IOException;
import java.io.Serializable;

final class CustomCheckpointMark implements UnboundedSource.CheckpointMark , Serializable {
    @Override
    public void finalizeCheckpoint() throws IOException {

    }
}
