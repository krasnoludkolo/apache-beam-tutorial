package org.apache.beam.examples;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;

final class PerWindowFiles extends FileBasedSink.FilenamePolicy implements ResourceId, ValueProvider<String> {
    public PerWindowFiles(ResourceId resource) {
    }
}
