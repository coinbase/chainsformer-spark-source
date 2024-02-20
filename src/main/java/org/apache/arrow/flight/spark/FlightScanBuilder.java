/*
 * Copyright (C) 2019 The flight-spark-source Authors
 * Copyright (C) 2024 Coinbase Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.spark;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.spark.datasource.DataSourceOptions;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.execution.arrow.FlightArrowUtils;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

import java.util.List;

public class FlightScanBuilder implements SupportsPushDownFilters, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanBuilder.class);
    private static final Joiner WHERE_JOINER = Joiner.on(" and ");
    private SchemaResult flightSchema;
    private FlightDescriptor descriptor;
    private StructType schema;
    private final FlightClientFactory clientFactory;
    private String sql;
    private Filter[] pushed;
    private final Broadcast<DataSourceOptions> dataSourceOptions;
    private final RetryTemplate retryTemplate;

    public FlightScanBuilder(Broadcast<DataSourceOptions> dataSourceOptions) {
        this.dataSourceOptions = dataSourceOptions;
        this.clientFactory = new FlightClientFactory(dataSourceOptions.value().getClientConfig());
        this.sql = dataSourceOptions.value().getQuery().getSql();
        this.retryTemplate = new RetryTemplateFactory().apply();
    }

    private SchemaResult getSchemaOnce(RetryContext context) {
        LOGGER.info(String.format("Attempting to get schema for the %d time", context.getRetryCount()));
        try (FlightClient client = clientFactory.apply()) {
            return client.getSchema(dataSourceOptions.getValue().getQuery().getSchemaDescriptor());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Scan build() {
        return new FlightScan(readSchema(), dataSourceOptions, clientFactory, sql);
    }

    public StructType readSchema() {
        if (schema == null) {
            schema = readSchemaImpl();
        }
        return schema;
    }

    private StructType readSchemaImpl() {
        if (flightSchema == null) {
            flightSchema = retryTemplate.execute(this::getSchemaOnce);
        }
        StructField[] fields = flightSchema.getSchema().getFields().stream()
                .map(field ->
                        new StructField(field.getName(),
                                FlightArrowUtils.fromArrowField(field),
                                field.isNullable(),
                                Metadata.empty()))
                .toArray(StructField[]::new);
        return new StructType(fields);
    }

    private String valueToString(Object value) {
        if (value instanceof String) {
            return String.format("'%s'", value);
        }
        return value.toString();
    }

    private boolean canBePushed(Filter filter) {
        if (filter instanceof IsNotNull) {
            return true;
        } else if (filter instanceof EqualTo) {
            return true;
        }
        if (filter instanceof GreaterThan) {
            return true;
        }
        if (filter instanceof GreaterThanOrEqual) {
            return true;
        }
        if (filter instanceof LessThan) {
            return true;
        }
        if (filter instanceof LessThanOrEqual) {
            return true;
        }
        LOGGER.error("Cant push filter of type " + filter.toString());
        return false;
    }

    private String generateWhereClause(List<Filter> pushed) {
        List<String> filterStr = Lists.newArrayList();
        for (Filter filter : pushed) {
            if (filter instanceof IsNotNull) {
                filterStr.add(String.format("isnotnull(\"%s\")", ((IsNotNull) filter).attribute()));
            } else if (filter instanceof EqualTo) {
                filterStr.add(String.format("\"%s\" = %s", ((EqualTo) filter).attribute(), valueToString(((EqualTo) filter).value())));
            } else if (filter instanceof GreaterThan) {
                filterStr.add(String.format("\"%s\" > %s", ((GreaterThan) filter).attribute(), valueToString(((GreaterThan) filter).value())));
            } else if (filter instanceof GreaterThanOrEqual) {
                filterStr.add(String.format("\"%s\" <= %s", ((GreaterThanOrEqual) filter).attribute(), valueToString(((GreaterThanOrEqual) filter).value())));
            } else if (filter instanceof LessThan) {
                filterStr.add(String.format("\"%s\" < %s", ((LessThan) filter).attribute(), valueToString(((LessThan) filter).value())));
            } else if (filter instanceof LessThanOrEqual) {
                filterStr.add(String.format("\"%s\" <= %s", ((LessThanOrEqual) filter).attribute(), valueToString(((LessThanOrEqual) filter).value())));
            }
            //todo fill out rest of Filter types
        }
        return WHERE_JOINER.join(filterStr);
    }

    private FlightDescriptor getDescriptor(String path) {
        String cmd = String.format("{\"table\": \"%s\"}", path);
        return FlightDescriptor.command(cmd.getBytes());
    }

    private void mergeWhereDescriptors(String whereClause) {
        sql = String.format("select * from (%s) where %s", sql, whereClause);
        descriptor = getDescriptor(sql);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> notPushed = Lists.newArrayList();
        List<Filter> pushed = Lists.newArrayList();
        for (Filter filter : filters) {
            boolean isPushed = canBePushed(filter);
            if (isPushed) {
                pushed.add(filter);
            } else {
                notPushed.add(filter);
            }
        }
        this.pushed = pushed.toArray(new Filter[0]);
        if (!pushed.isEmpty()) {
            String whereClause = generateWhereClause(pushed);
            mergeWhereDescriptors(whereClause);
            try (FlightClient client = clientFactory.apply()) {
                flightSchema = client.getSchema(descriptor);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return notPushed.toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushed;
    }

    @Override
    public void close() throws Exception {
        clientFactory.close();
    }
}
