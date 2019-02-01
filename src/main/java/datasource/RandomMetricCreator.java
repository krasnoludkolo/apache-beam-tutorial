package datasource;

import analyser.Metric;

import java.util.Random;

final class RandomMetricCreator {

    private Random r = new Random();

    private String[] usernames = {"a","b","c","d","e","f","g","h"};
    private String[] metrics = {"Speed","Not speed", "Also not speed"};
    private int[] maxMetricsValue = {100,50,100};


    Metric create(long timestamp) {
        int i = r.nextInt(metrics.length);
        String metric = metrics[i];
        long[] value = new long[]{r.nextInt(maxMetricsValue[i])};
        String user = usernames[r.nextInt(usernames.length)];
        return new Metric(metric,timestamp,value,user);
    }
}
