/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.resource.transaction.spi.DdlTransactionIsolator;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.internal.exec.ImprovedExtractionContextImpl;
import org.hibernate.tool.schema.internal.exec.JdbcContext;

import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

public class ReactiveImprovedExtractionContextImpl extends ImprovedExtractionContextImpl implements ReactiveExtractionContext {

	private static CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveImprovedExtractionContextImpl.class );

	private final ServiceRegistry serviceRegistry;
	private VertxInstance vertxSupplier;
	private ReactiveConnectionPool service;

	public ReactiveImprovedExtractionContextImpl(
			ServiceRegistry registry,
			Namespace.Name name,
			DatabaseObjectAccess databaseObjectAccess) {
		super(
				registry,
				registry.getService( JdbcEnvironment.class ),
				new DdlTransactionIsolator() {
					@Override
					public JdbcContext getJdbcContext() {
						return null;
					}

					@Override
					public void prepare() {
					}

					@Override
					public Connection getIsolatedConnection() {
						return null;
					}

					@Override
					public void release() {
					}
				},
				name.getCatalog(),
				name.getSchema(),
				databaseObjectAccess
		);
		this.serviceRegistry = registry;
	}

	@Override
	public <T> T getQueryResults(
			String queryString,
			ResultSetProcessor<T> resultSetProcessor) throws SQLException {

		return resultSetProcessor.process( getQueryResultSet( queryString ) );
	}

	@Override
	public ResultSet getQueryResultSet(String queryString) {
		service = serviceRegistry.getService( ReactiveConnectionPool.class );

		return service.getConnection()
				.thenCompose( c -> c.selectJdbc( queryString, new Object[0] ) )
				.handle( (resultSet, err) -> {
					logSqlException(
							err,
							() -> "could not execute query ", queryString
					);
					return returnOrRethrow( err, resultSet );
				})
				.toCompletableFuture()
				.join();
	}
}
