/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.persistence.CascadeType;
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
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.testing.SessionFactoryManager;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.tool.schema.JdbcMetadaAccessStrategy;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @author Gail Badner
 */

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class SchemaUpdatePostgreSqlTest extends AbstractReactiveTest {

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

	private Object session;

	private ReactiveConnection connection;

	@Test
	public void testUpdate(TestContext context) {
		final ASimpleNext aSimple = new ASimpleNext();
		aSimple.aValue = 9;
		aSimple.aStringValue = "abc";
		aSimple.data = "Data";

		final AOther aOther = new AOther();
		aOther.id1Int = 1;
		aOther.id2String = "other";
		aOther.anotherString = "another";

		final AAnother aAnother = new AAnother();
		aAnother.description = "description";

		aSimple.aOther = aOther;
		aSimple.aAnother = aAnother;

		test(
				context,
						setup(
								Arrays.asList( ASimpleNext.class, AOther.class, AAnother.class ),
													"update",
													false

						)
						.thenCompose( v -> openSession()
								.thenCompose( s -> voidFuture()
										.thenCompose( v1 -> s.persist( aSimple ) )
										.thenCompose( v1 -> s.flush() )
										.thenAccept( v1 -> s.clear() )
										.thenAccept( v1 -> factoryManager.stop() )
								)
						)
		);
	}

	@Before
	public void before(TestContext context) {
		setup(
				Arrays.asList( ASimpleFirst.class, AOther.class ),
				"create",
				true
		);
	}

	@After
	public void after(TestContext context) {
		setup(
				Arrays.asList( ASimpleNext.class, AOther.class, AAnother.class ),
				"drop",
				true
		);
	}

	protected SessionFactoryManager getSessionFactoryManager() {
		return factoryManager;
	}

	private CompletionStage<Void> setup(
			Collection<Class<?>> entityClasses,
			String hbm2DdlOption,
			boolean closeAfterSessionFactoryStarted) {
		CompletableFuture<Void> future = new CompletableFuture<Void>();
		vertxContextRule.vertx()
				.executeBlocking(
						// schema generation is a blocking operation and so it causes an
						// exception when run on the Vert.x event loop. So call it using
						// Vertx.executeBlocking()
						p -> startFactoryManager( p, entityClasses, hbm2DdlOption ),
						event -> {
							if ( closeAfterSessionFactoryStarted ) {
								factoryManager.stop();
							}
							if ( event.succeeded() ) {
								future.complete();
							}
							else {
								future.fail( event.cause() );
							}
						}
				);
		return voidFuture();
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

	protected Configuration constructConfiguration(String hbm2DdlOption) {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, hbm2DdlOption );
		configuration.setProperty( Settings.DEFAULT_SCHEMA, "public" );
		configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, jdbcMetadataExtractorStrategy );

		return configuration;
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

		private String aStringValue;

		private String data;

		@ManyToOne(cascade = CascadeType.ALL)
		private AOther aOther;

		@ManyToOne(cascade = CascadeType.ALL)
		private AAnother aAnother;
	}

	@Entity(name = "AOther")
	@IdClass(AOtherId.class)
	public static class AOther {
		@Id
		private int id1Int;

		@Id
		private String id2String;

		private String anotherString;
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

	@Entity(name = "AAnother")
	public static class AAnother {
		@Id
		@GeneratedValue
		private Integer id;

		private String description;
	}
}
