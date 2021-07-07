/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.provider.service.ReactiveGenerationTarget;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.SessionFactoryManager;
import org.hibernate.tool.schema.spi.SchemaManagementTool;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public abstract class AbstractReactiveTest {

	@Rule
	public Timeout rule = Timeout.seconds( 5 * 60 );

	@ClassRule
	public static RunTestOnContext vertxContextRule = new RunTestOnContext( () -> {
		VertxOptions options = new VertxOptions();
		options.setBlockedThreadCheckInterval( 5 );
		options.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		return Vertx.vertx( options );
	} );

	private Object session;

	private ReactiveConnection connection;

	protected static void test(TestContext context, CompletionStage<?> work) {
		test( context.async(), context, work );
	}

	/**
	 * For when we need to create the {@link Async} in advance
	 */
	protected static void test(Async async, TestContext context, CompletionStage<?> work) {
		work.whenComplete( (res, err) -> {
			if ( err != null ) {
				context.fail( err );
			}
			else {
				async.complete();
			}
		} );
	}

	protected static void test(TestContext context, Uni<?> uni) {
		test( context.async(), context, uni );
	}

	/**
	 * For when we need to create the {@link Async} in advance
	 */
	public static void test(Async async, TestContext context, Uni<?> uni) {
		uni.subscribe().with(
				res -> async.complete(),
				context::fail
		);
	}

	private static boolean doneTablespace;

	protected Configuration constructConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( Settings.URL, DatabaseConfiguration.getJdbcUrl() );
		if ( DatabaseConfiguration.dbType() == DatabaseConfiguration.DBType.DB2 && !doneTablespace ) {
			configuration.setProperty(Settings.HBM2DDL_IMPORT_FILES, "/db2.sql");
			doneTablespace = true;
		}
		//Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty(Settings.SHOW_SQL, "false") );
		configuration.setProperty( Settings.FORMAT_SQL, System.getProperty(Settings.FORMAT_SQL, "false") );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, System.getProperty(Settings.HIGHLIGHT_SQL, "true") );
		return configuration;
	}

	protected void addServices(StandardServiceRegistryBuilder builder) {}

	/*
	 * MySQL doesn't implement 'drop table cascade constraints'.
	 *
	 * The reason this is a problem in our test suite is that we
	 * have lots of different schemas for the "same" table: Pig, Author, Book.
	 * A user would surely only have one schema for each table.
	 */
	protected void configureServices(StandardServiceRegistry registry) {
		if ( dbType() == DatabaseConfiguration.DBType.MYSQL ) {
			registry.getService( ConnectionProvider.class ); //force the NoJdbcConnectionProvider to load first
			registry.getService( SchemaManagementTool.class )
					.setCustomDatabaseGenerationTarget( new ReactiveGenerationTarget( registry) {
						@Override
						public void prepare() {
							super.prepare();
							accept("set foreign_key_checks = 0");
						}
						@Override
						public void release() {
							accept("set foreign_key_checks = 1");
							super.release();
						}
					} );
		}
	}

	@After
	public void after(TestContext context) {
		test( context, closeSession( session )
				.thenAccept( v -> session = null )
				.thenCompose( v -> closeSession( connection ) )
				.thenAccept( v -> connection = null ) );
	}

	protected static CompletionStage<Void> closeSession(Object closable) {
		if ( closable instanceof ReactiveConnection ) {
			return ( (ReactiveConnection) closable ).close();
		}
		if ( closable instanceof Mutiny.Session) {
			Mutiny.Session mutiny = (Mutiny.Session) closable;
			if ( mutiny.isOpen() ) {
				return mutiny.close().subscribeAsCompletionStage();
			}
		}
		if ( closable instanceof Stage.Session) {
			Stage.Session stage = (Stage.Session) closable;
			if ( stage.isOpen() ) {
				return stage.close();
			}
		}
		return voidFuture();
	}

	protected abstract SessionFactoryManager getSessionFactoryManager();

	protected Stage.SessionFactory getSessionFactory() {
		return getSessionFactoryManager().getHibernateSessionFactory().unwrap( Stage.SessionFactory.class );
	}

	/**
	 * Close the existing open session and create a new {@link Stage.Session}
	 *
	 * @return a new Stage.Session
	 */
	protected CompletionStage<Stage.Session> openSession() {
		return closeSession( session )
				.thenApply( v -> {
					Stage.Session newSession = getSessionFactory().openSession();
					this.session = newSession;
					return newSession;
				} );
	}

	protected CompletionStage<ReactiveConnection> connection() {
		return getSessionFactoryManager().getReactiveConnectionPool().getConnection().thenApply( c -> connection = c );
	}

	/**
	 * Close the existing open session and create a new {@link Mutiny.Session}
	 *
	 * @return a new Mutiny.Session
	 */
	protected Uni<Mutiny.Session> openMutinySession() {
		return Uni.createFrom().completionStage( closeSession( session ) )
				.replaceWith( () -> {
					Mutiny.Session newSession = getMutinySessionFactory().openSession();
					this.session = newSession;
					return newSession;
				} );
	}

	protected Mutiny.SessionFactory getMutinySessionFactory() {
		return getSessionFactoryManager().getHibernateSessionFactory().unwrap( Mutiny.SessionFactory.class );
	}
}
