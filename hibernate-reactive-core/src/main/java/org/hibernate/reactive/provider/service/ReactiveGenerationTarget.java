/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.internal.exec.GenerationTarget;
import org.hibernate.tool.schema.internal.exec.GenerationTargetToDatabase;

/**
 * Adaptor that redirects DDL generated by the schema export
 * tool to the re+active connection.
 *
 * @author Gavin King
 */
public class ReactiveGenerationTarget implements GenerationTarget {
	private final ServiceRegistry registry;
	private VertxInstance vertxSupplier;
	private ReactiveConnectionPool service;
	private Set<String> statements;
	private List<String> commands = new ArrayList<>();

	private volatile CountDownLatch done;

	CoreMessageLogger log = CoreLogging.messageLogger( GenerationTargetToDatabase.class );

	public ReactiveGenerationTarget(ServiceRegistry registry) {
		this.registry = registry;
	}

	@Override
	public void prepare() {
		service = registry.getService( ReactiveConnectionPool.class );
		vertxSupplier = registry.getService( VertxInstance.class );
		statements = new HashSet<>();
		done = new CountDownLatch( 1 );
	}

	@Override
	public void accept(String command) {
		// avoid executing duplicate DDL statements
		// (hack specifically to avoid multiple
		// inserts into a sequence emulation table)
		if ( statements.add( command ) ) {
			commands.add( command );
		}
	}

	@Override
	public void release() {
		statements = null;
		if ( commands != null ) {
			vertxSupplier.getVertx().getOrCreateContext().runOnContext( v1 ->
					service.getConnection()
						.thenCompose( this::executeCommands )
						.whenComplete( (v, e) -> {
							if ( e != null ) {
								log.warnf( "HRX000021: DDL command failed [%s]", e.getMessage() );
							}
							done.countDown();
						} )
			);

			if ( done != null ) {
				try {
					done.await();
				}
				catch (InterruptedException e) {
					log.warnf( "Interrupted while performing schema export operations", e.getMessage() );
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private CompletionStage<Void> executeCommands(ReactiveConnection reactiveConnection) {
		CompletionStage<Void> result = CompletionStages.voidFuture();
		for ( String command : commands ) {
			result = result.thenCompose(  v -> reactiveConnection.execute( command )
					.handle( (r, e) -> {
						if ( e != null ) {
							log.warnf( "HRX000021: DDL command failed [%s]", e.getMessage() );
						}
						return null;
					} )
			);
		}
		return result
				.whenComplete( (v, e) -> reactiveConnection.close() );
	}
}
