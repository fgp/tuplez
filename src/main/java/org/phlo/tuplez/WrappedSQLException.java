package org.phlo.tuplez;

import java.sql.SQLException;

/**
 * WrappedSQLException in a unchecked exception class
 * which wraps an SQL exception (which is checked),
 * and thus allows SQL exception to be thrown "through"
 * code that doesn't declare "throws SQLException". 
 */
@SuppressWarnings("serial")
final class WrappedSQLException extends RuntimeException {
	private final SQLException m_sqlException;
	
	WrappedSQLException(final SQLException sqlException) {
		super(sqlException.getMessage(), sqlException);
		m_sqlException = sqlException;
	}
	
	SQLException getSQLException() {
		return m_sqlException;
	}
}
