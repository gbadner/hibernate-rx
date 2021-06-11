/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.tool.schema.extract.internal.AbstractInformationExtractorImpl;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;

public class ReactiveInformationExtractorImpl extends AbstractInformationExtractorImpl {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveInformationExtractorImpl.class );
	private final ReactiveExtractionContext extractionContext;

	public ReactiveInformationExtractorImpl(ExtractionContext extractionContext) {
		super( extractionContext );
		this.extractionContext = (ReactiveExtractionContext) extractionContext;
	}

	@Override
	protected ResultSet getCatalogsResultSet() throws SQLException {
		return extractionContext.getQueryResultSet(
				String.format( "select %s from information_schema.information_schema_catalog_name", getCatalogColumnName() )
		);
	}

	@Override
	protected String getCatalogColumnName() {
		return "catalog_name";
	}

	@Override
	protected ResultSet getSchemasResultSet(String catalogFilter, String schemaFilter) throws SQLException {
		return null;
	}

	@Override
	protected String getTablesResultSetCatalogNameColumn() {
		return "table_catalog";
	}

	@Override
	protected String getTablesResultSetSchemaNameColumn() {
		return "table_schema";
	}

	@Override
	protected String getTablesResultSetTableNameColumn() {
		return "table_name";
	}

	@Override
	protected String getTablesResultSetTableTypesTableConstant() {
		return "BASE TABLE";
	}

	@Override
	protected String getTablesResultSetTableTypeColumn() {
		return "table_type";
	}

	@Override
	protected String getTablesResultSetRemarksColumn() {
		return "no_such_column_";
	}

	@Override
	protected ResultSet getTablesResultSet(
			String catalogFilter, String schemaFilter, String tableNameFilter, String[] tableTypes)
			throws SQLException {
		final StringBuilder sb = new StringBuilder( "select ")
				.append( getTablesResultSetCatalogNameColumn() )
				.append( ", " )
				.append( getTablesResultSetSchemaNameColumn() )
				.append( ", " )
				.append( getTablesResultSetTableNameColumn() )
				.append( ", " )
				.append( getTablesResultSetTableTypeColumn() )
				.append( ", null as " )
				.append( getTablesResultSetRemarksColumn() )
				.append( " from information_schema.tables " );
		if ( catalogFilter != null || schemaFilter != null || ( tableTypes != null && tableTypes.length != 0 ) ) {
			sb.append( "where " );
			boolean requiresAnd = false;
			if ( catalogFilter != null ) {
				if ( requiresAnd ) {
					sb.append( "and " );
				}
				sb.append( "table_catalog = " ).append( '\'' ).append( catalogFilter ).append( "' " );
				requiresAnd = true;
			}
			if ( schemaFilter != null ) {
				if ( requiresAnd ) {
					sb.append( "and " );
				}
				sb.append( "table_schema = " ).append( '\'' ).append( schemaFilter ).append( "' " );
				requiresAnd = true;
			}
			if ( tableTypes != null && tableTypes.length > 0 ) {
				if ( requiresAnd ) {
					sb.append( "and " );
				}
				sb.append( "table_type in ( " );
				boolean isFirst = true;
				for ( String tableType : tableTypes ) {
					if ( !isFirst ) {
						sb.append( ',' );
					}
					sb.append( '\'' ).append(
							tableType.equals( "TABLE" ) ? getTablesResultSetTableTypesTableConstant() : tableType
					).append( "' " );
					isFirst = false;
				}
				sb.append( " ) " );
				requiresAnd = true;
			}
		}
		return extractionContext.getQueryResultSet(
				 sb.toString()
		);
	}

	@Override
	protected String getColumnsResultSetTableNameColumn() {
		return "table_name";
	}

	@Override
	protected String getColumnsResultSetColumnNameColumn() {
		return "column_name";
	}

	@Override
	protected String getColumnsResultSetDataTypeColumn() {
		return "type_code_does_not_exist_";
	}

	@Override
	protected String getColumnsResultSetTypeNameColumn() {
		return "data_type";
	}

	@Override
	protected String getColumnsResultSetColumnSizeColumn() {
		return null;
	}

	@Override
	protected String getColumnsResultSetDecimalDigitsColumn() {
		return null;
	}

	@Override
	protected String getColumnsResultSetIsNullableColumn() {
		return null;
	}

	@Override
	protected ResultSet getColumnsResultSet(
			String catalogFilter, String schemaFilter, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		return null;
	}

	@Override
	protected ResultSet getPrimaryKeysResultSet(
			String catalogFilter, String schemaFilter, Identifier tableName) throws SQLException {
		return null;
	}

	@Override
	protected String getPrimaryKeysResultSetPrimaryKeyNameColumn() {
		return null;
	}

	@Override
	protected String getPrimaryKeysResultSetColumnPositionColumn() {
		return null;
	}

	@Override
	protected String getPrimaryKeysResultSetColumnNameColumn() {
		return null;
	}

	@Override
	protected ResultSet getIndexInfoResultSet(
			String catalogFilter, String schemaFilter, Identifier tableName, boolean unique, boolean approximate)
			throws SQLException {
		return null;
	}

}
