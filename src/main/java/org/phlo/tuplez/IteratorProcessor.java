package org.phlo.tuplez;

import java.util.Iterator;

import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Processor for result iterators produced by iterate().
 * 
 * Required because
 * {@link org.springframework.jdbc.core.JdbcTemplate}
 * and
 * {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}
 * close() the ResultSet after the {@link ResultSetExtractor} returns.
 */
public interface IteratorProcessor<OutputType, ResultType> {
	public ResultType processIterator(Iterator<OutputType> iterator);
}
