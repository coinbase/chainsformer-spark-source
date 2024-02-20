package org.apache.arrow.flight.spark.datasource;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.spark.sql.connector.read.streaming.Offset;

import java.util.List;

public interface Query {
    String getSql();
    String getMicroBatchQuery(Offset start, Offset end);
    List<String> getBatchQueries();
    long getStartPosition();
    int getMaxBatchSize();
    Action getTipAction();
    Action getStartAction();
    FlightDescriptor getSchemaDescriptor();
}
