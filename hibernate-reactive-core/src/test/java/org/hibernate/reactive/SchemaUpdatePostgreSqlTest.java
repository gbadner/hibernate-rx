/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Index;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.provider.service.ReactiveGenerationTarget;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.testing.SessionFactoryManager;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.tool.schema.JdbcMetadaAccessStrategy;
import org.hibernate.tool.schema.spi.SchemaManagementTool;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * @author Gail Badner
 */

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class SchemaUpdatePostgreSqlTest {

	@Parameterized.Parameters
	public static Collection<String> parameters() {
		return Arrays.asList(
				JdbcMetadaAccessStrategy.GROUPED.toString(), JdbcMetadaAccessStrategy.INDIVIDUALLY.toString()
		);
	}

	@Parameterized.Parameter
	public String jdbcMetadataExtractorStrategy;

	public static SessionFactoryManager factoryManager = new SessionFactoryManager();

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Rule
	public Timeout timeoutRule = Timeout.seconds( 5 * 60 );

	@ClassRule
	public static RunTestOnContext vertxContextRule = new RunTestOnContext( () -> {
		VertxOptions options = new VertxOptions();
		options.setBlockedThreadCheckInterval( 5 );
		options.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		Vertx vertx = Vertx.vertx( options );
		return vertx;
	} );

	private Object session;

	private ReactiveConnection connection;

	@Test
	public void testUpdate(TestContext context) {
		setup(
				context,
				Arrays.asList( ASimpleNext.class, AOther.class, AAnother.class ),
				"update"
		);
	}

	@Before
	public void before(TestContext context) {
		Collection<Class<?>> entityClasses = new HashSet<>();
		entityClasses.add( ASimpleFirst.class );
		entityClasses.add( AOther.class );
		setup(
				context,
				Arrays.asList( ASimpleFirst.class, AOther.class ),
				"create"
		);
	}

	private void setup(TestContext context, Collection<Class<?>> entityClasses, String hbm2DdlOption) {
		Async async = context.async();
		vertxContextRule.vertx()
				.executeBlocking(
						// schema generation is a blocking operation and so it causes an
						// exception when run on the Vert.x event loop. So call it using
						// Vertx.executeBlocking()
						p -> startFactoryManager( p, entityClasses, hbm2DdlOption ),
						event -> {
							factoryManager.stop();
							if ( event.succeeded() ) {
								async.complete();
							}
							else {
								context.fail( event.cause() );
							}
						}
				);
	}

	private void startFactoryManager(Promise<Object> p, Collection<Class<?>> entityClasses, String hbm2DdlOption) {
		try {
			factoryManager.start( () -> createHibernateSessionFactory( entityClasses, hbm2DdlOption ) );
			p.complete();
		}
		catch (Throwable e) {
			p.fail( e );
		}
	}

	private SessionFactory createHibernateSessionFactory(Collection<Class<?>> entityClasses, String hbm2DdlOption) {
		Configuration configuration = constructConfiguration( hbm2DdlOption );
		for ( Class<?> entityClass : entityClasses ) {
			configuration.addAnnotatedClass( entityClass );
		}
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.addService( VertxInstance.class, (VertxInstance) () -> vertxContextRule.vertx() )
				.applySettings( configuration.getProperties() );
		StandardServiceRegistry registry = builder.build();
		configureServices( registry );
		SessionFactory sessionFactory = configuration.buildSessionFactory( registry );
		return sessionFactory;
	}

	private static boolean doneTablespace;

	protected Configuration constructConfiguration(String hbm2DdlOption) {
		Configuration configuration = new Configuration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, hbm2DdlOption );
		configuration.setProperty( Settings.URL, DatabaseConfiguration.getJdbcUrl() );
		if ( DatabaseConfiguration.dbType() == DatabaseConfiguration.DBType.DB2 && !doneTablespace ) {
			configuration.setProperty(Settings.HBM2DDL_IMPORT_FILES, "/db2.sql");
			doneTablespace = true;
		}
		//Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty(Settings.SHOW_SQL, "false") );
		configuration.setProperty( Settings.FORMAT_SQL, System.getProperty(Settings.FORMAT_SQL, "false") );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, System.getProperty(Settings.HIGHLIGHT_SQL, "true") );
		configuration.setProperty( Settings.DEFAULT_SCHEMA, "public" );
		configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, jdbcMetadataExtractorStrategy );

		return configuration;
	}
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

	@Entity(name = "ASimple")
	@Table(name = "ASimple", indexes = @Index( columnList = "aValue ASC, aStringValue DESC"))
	public static class ASimpleFirst {
		@Id
		@GeneratedValue
		private Integer id;
		private Integer aValue;
		private String aStringValue;
		@ManyToOne
		private AOther aOther;
	}

	@Entity(name = "AOther")
	@IdClass(AOtherId.class)
	public static class AOther {
		@Id
		private int id1Int;

		@Id
		private String id2String;
	}

	public static class AOtherId implements Serializable {
		private int id1Int;
		private String id2String;

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			AOtherId aOtherId = (AOtherId) o;
			return id1Int == aOtherId.id1Int && id2String.equals( aOtherId.id2String );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id1Int, id2String );
		}
	}

	@Entity(name = "ASimple")
	@Table(name = "ASimple", indexes = {
			@Index( columnList = "aValue ASC, aStringValue DESC"),
			@Index( columnList = "aValue DESC, data ASC")
	},
	uniqueConstraints = { @UniqueConstraint( name = "uniq", columnNames = "aStringValue")})
	public static class ASimpleNext {
		@Id
		@GeneratedValue
		private Integer id;

		private Integer aValue;

		@Column
		private String aStringValue;

		@ManyToOne(cascade = CascadeType.ALL)
		private AOther aOther;

		@ManyToOne(cascade = CascadeType.ALL)
		private AAnother aAnother;

		private String data;
	}

	@Entity(name = "AAnother")
	public static class AAnother {
		@Id
		@GeneratedValue
		private Integer id;

		private String description;
	}
}
