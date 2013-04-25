package com.servicenow.syseng.metadata;

/**
 * Created with IntelliJ IDEA.
 * User: SERVICE-NOW\chi.yao
 * Date: 4/23/13
 * Time: 2:30 PM
 * To change this template use File | Settings | File Templates.
 */

import com.servicenow.syseng.datamodel.CanonicalMetrics;

public class TestHostMetricsExtractor extends MetaDataExtractor {

    public final KeyValuePair extract(CanonicalMetrics cm) {
        String host = cm.fqdn;
        String metrics = cm.metricName;
        return new KeyValuePair(host,metrics);
    }

    public TestHostMetricsExtractor() {
        super("HostMetrics"); // map name is HostMetrics
    }

}