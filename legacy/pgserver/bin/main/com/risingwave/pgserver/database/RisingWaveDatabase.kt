package com.risingwave.pgserver.database

import com.risingwave.catalog.CatalogService
import com.risingwave.common.exception.PgException
import com.risingwave.execution.context.ExecutionContext
import com.risingwave.execution.context.FrontendEnv
import com.risingwave.execution.context.SessionConfiguration
import com.risingwave.pgwire.database.Database
import com.risingwave.pgwire.database.PgResult

/**
 * Create one for each session.
 */
class RisingWaveDatabase internal constructor(
  var frontendEnv: FrontendEnv,
  var database: String,
  user: String,
  var sessionConfiguration: SessionConfiguration
) : Database {
  @Throws(PgException::class)
  override suspend fun runStatement(sqlStmt: String): PgResult {
    return try {
      val executionContext = ExecutionContext.builder()
        .withDatabase(database)
        .withSchema(CatalogService.DEFAULT_SCHEMA_NAME)
        .withFrontendEnv(frontendEnv)
        .withSessionConfig(sessionConfiguration)
        .build()
      QueryExecution(executionContext, sqlStmt).call()
    } catch (e: Throwable) {
      throw PgException.from(e)
    }
  }

  override val serverEncoding: String
    get() = "UTF-8"
}
