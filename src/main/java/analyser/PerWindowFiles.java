package analyser;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

final class PerWindowFiles extends FileBasedSink.FilenamePolicy {
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinuteSecond();
    private final ResourceId prefix;

    PerWindowFiles(ResourceId prefix) {
        this.prefix = prefix;
    }

    private String filenamePrefixForWindow(IntervalWindow window) {
        String filePrefix = prefix.isDirectory() ? "" : prefix.getFilename();
        return String.format(
                "%s---%s---%s", filePrefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
    }

    @Override
    public ResourceId windowedFilename(
            int shardNumber,
            int numShards,
            BoundedWindow window,
            PaneInfo paneInfo,
            FileBasedSink.OutputFileHints outputFileHints) {
        IntervalWindow intervalWindow = (IntervalWindow) window;
        String filename =
                String.format(
                        "%s-%s-of-%s%s",
                        filenamePrefixForWindow(intervalWindow),
                        shardNumber,
                        numShards,
                        outputFileHints.getSuggestedFilenameSuffix());
        return prefix.getCurrentDirectory().resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(
            int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
        throw new UnsupportedOperationException("Unsupported.");
    }
}
