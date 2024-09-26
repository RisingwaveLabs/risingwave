/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class PostgresErrorHandler extends ErrorHandler {

    public PostgresErrorHandler(
            PostgresConnectorConfig connectorConfig,
            ChangeEventQueue<?> queue,
            ErrorHandler replacedErrorHandler) {
        super(PostgresConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, SQLException.class);
    }

    // Introduced for testing only
    @Override
    protected boolean isRetriable(Throwable throwable) {
        return super.isRetriable(throwable);
    }
}
