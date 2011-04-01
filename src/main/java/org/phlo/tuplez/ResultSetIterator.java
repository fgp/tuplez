package org.phlo.tuplez;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Uses an instance of {@link ResultSetMapper} to provide an
 * iterator facade around JDBC {@link ResultSet} instances.
 *
 * @param <OutputType> type representing {@link ResultSet} rows
 */
final class ResultSetIterator<OutputType> implements Iterator<OutputType> {
	private final ResultSet m_resultSet;
	private final ResultSetMapper<OutputType> m_resultSetMapper;
	private OutputType m_next;
	
	/**
	 * Creates an iterator over the {@link ResultSet}'s rows by mapping
	 * each row to an instance of OutputType using the resultSetMapper.
	 * 
	 * @param resultSet the {@link ResultSet} to iterate over
	 * @param resultSetMapper the mapper which converts rows instances of OutputType.
	 */
	ResultSetIterator(final ResultSet resultSet, final ResultSetMapper<OutputType> resultSetMapper) {
		m_resultSet = resultSet;
		m_resultSetMapper = resultSetMapper;
		m_next = extractNext();
	}
	
	public boolean hasNext() {
		return (m_next != null);
	}

	public OutputType next() {
		if (m_next == null)
			throw new NoSuchElementException("no more rows available");
		final OutputType current = m_next;
		m_next = extractNext();
		return current;
	}

	public void remove() {
		throw new UnsupportedOperationException(getClass().getName() + " does not support remove()");
	}
	
	private OutputType extractNext() {
		try {
			if (!m_resultSet.next())
				return null;
			
			return m_resultSetMapper.mapCurrentRow(m_resultSet);
		}
		catch (final SQLException e) {
			throw new WrappedSQLException(e);
		}
	}
}
