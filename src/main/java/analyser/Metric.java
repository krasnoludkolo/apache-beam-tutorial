package analyser;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public final class Metric {

    private String name;
    private long timestamp;
    private long[] values;
    private String user;

    public Metric(String name, long timestamp, long[] values, String user) {
        this.name = name;
        this.timestamp = timestamp;
        this.values = values;
        this.user = user;
    }
}
