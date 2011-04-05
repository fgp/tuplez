package org.phlo.tuplez;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import javax.sql.DataSource;

import org.phlo.tuplez.operation.*;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.support.*;
import org.springframework.jdbc.core.namedparam.*;

/**
 * Executes concrete {@link Operation}s.
 * <p>
 * Executor instances are created from {@link DataSource}s,
 * and contain instance methods which execute concrete
 * {@link Operation} implementations.
 * <p>
 * Executor instances or typically created by Spring.
 * This is e.g. done by putting the following into your
 * application context configuration XML (assuming that
 * there is a bean named "DataSource" which defines
 * your database connection).
 *<blockquote><pre>{@literal
 *<bean id="DatabaseExecutor"
 *      class="org.phlo.tuplez.Executor"
 *>
 *    <property name="DataSource" ref="DataSource"/>
 *</bean>
 *}</pre></blockquote>
 *
 * @see Operation
 */
public class Executor {
	/* The named-parameter JDBC template used to execute operations */
	private NamedParameterJdbcTemplate m_npJdbcTemplate;

	/* The parameter source for default.* input parameters */
	private Object m_defaultInput;
	
	/**
	 * Allows construction of {@link Executor} instances
	 * as Java Beans.
	 */
	public Executor() {
		m_npJdbcTemplate = null;
	}
	
	/**
	 * Creates an {@link Executor} instance for the specified
	 * {@link DataSource}.
	 * 
	 * @param dataSource data source to use
	 */
	public Executor(final DataSource dataSource) {
		setDataSource(dataSource);
	}
	
	/**
	 * Sets the executor's {@link DataSource}. Usually not
	 * called directly, but instead called by Spring when
	 * your executor bean definition contains
	 *<blockquote><pre>{@literal
	 *    <property name="DataSource" ref="NameOfYourDataSource"/>
	 *}</pre></blockquote>
	 *
	 * @param dataSource data source to use
	 */
	@Required
	public void setDataSource(final DataSource dataSource) {
		m_npJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
		
	/**
	 * Set the executor's default input parameter bean.
	 * <p>
	 * This object is queried when database operations
	 * access default.* parameters.
	 * 
	 * @param defaultInput default input parameter bean
	 */
	public void setDefaultInput(final Object defaultInput) {
		m_defaultInput = defaultInput;
	}
	
	/**
	 * Returns the executor's default input parameter bean.
	 * 
	 * @see #setDefaultInput(Object)
	 * 
	 * @return default input parameter bean
	 */
	public Object getDefaultInput() {
		return m_defaultInput;
	}
	
	/**
	 * Returns the underlying JDBC operations instances.
	 * 
	 * Can be used to execute queries which are not
	 * represented by concrete implementations of
	 * {@link Operation}
	 * 
	 * @return the underlying JDBC operations instance
	 */
	public JdbcOperations getJdbcOperations() {
		return m_npJdbcTemplate.getJdbcOperations();
	}

	/**
	 * Executes a database operation using the provided
	 * instance of {@code InputType} to fill in parameters,
	 * passes an iterator over the resulting instances of
	 * {@code OutputType} to the {@code iteratorProcessor},
	 * and returns the {@code iteratorProcessor}'s result
	 * of type {@code ResultType}.
	 * <p>
	 * If the database operation requires no input (meaning
	 * {@code InputType} is {@link Void}) you must use
	 * {@link #iterate(Class, IteratorProcessor) iterate(opClass, iteratorProcessor)}
	 * instead, since {@link Void} is uninstantiable.
	 * <p>
	 * It's usually easier to use
	 * {@link #collection(Class, Object) collection(opClass, input)}
	 * instead of using this method directly
	 * 
	 * @see Operation
	 * @see #iterate(Class opClass, IteratorProcessor iteratorProcessor)
	 * @see #collection(Class opClass, Object input)
	 * @see #collection(Class opClass)
	 * 
	 * @param <InputType> the {@link Operation Operation&lt;InputType, OutputType&gt;}'s input type
	 * @param <ConcreteInputType> the actual (concrete) implementation of {@code InputType}
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;InputType, OutputType&gt;}
	 * @param <OutputType> the {@link Operation Operation&lt;InputType, OutputType&gt;}'s output type
	 *                      and {@link IteratorProcessor IteratorProcessor&lt;OutputType, ResultType&gt;}'s element type
	 * @param <ResultType> the {@link IteratorProcessor IteratorProcessor&lt;OutputType, ResultType&gt;}'s result type
	 * @param opClass the concrete {@link Operation Operation&lt;InputType, OutputType&gt;} implementation
	 * @param input the {@code ConcreteInputType} instance used to fill in the operation's parameters
	 * @param iteratorProcessor the {@link IteratorProcessor IteratorProcessor&lt;OutputType, ResultType&gt;} instance
	 * @return the {@code iteratorProcessor}'s result of type {@code ResultType}
	 */
	public <
		InputType,
		ConcreteInputType extends InputType, OutputType,
		OpClassType extends Operation<InputType, OutputType>,
		ResultType
	>
	ResultType iterate(
		final Class<OpClassType> opClass,
		final ConcreteInputType input,
		final IteratorProcessor<OutputType, ResultType> iteratorProcessor
	) {
		InputMapper<InputType> inputMapper = InputMapper.getInstance(
			opClass,
			(m_defaultInput != null) ? m_defaultInput.getClass() : null
		);
		
		return m_npJdbcTemplate.query(
			StatementMapper.getInstance(opClass).getStatement(input),
			inputMapper.mapInput(input, m_defaultInput),
			new ResultSetExtractor<ResultType>() {
				public ResultType extractData(final ResultSet resultSet) throws SQLException
				{
					try {
						return iteratorProcessor.processIterator(
							new ResultSetIterator<OutputType>(
								resultSet,
								ResultSetMapper.getInstance(opClass, resultSet)
							)
						);
					}
					catch (WrappedSQLException e) {
						throw e.getSQLException();
					}
				}
				
			}
		);
	}
	
	/**
	 * Executes a database operation which requires no input,
	 * passes an iterator over the resulting instances of
	 * {@code OutputType} to the {@code iteratorProcessor},
	 * and returns the {@code iteratorProcessor}'s result
	 * of type {@code ResultType}.
	 * <p>
	 * If the database operation requires input (meaning
	 * {@code InputType} is not {@link Void}) you must use
	 * {@link #iterate(Class, Object, IteratorProcessor) iterate(opClass, input, iteratorProcessor)}
	 * instead.
	 * <p>
	 * It's usually easier to use the member
	 * {@link #collection(Class) collection(opClass)} instead
	 * of using this method directly.
	 * 
	 * @see Operation
	 * @see #iterate(Class opClass, Object input, IteratorProcessor iteratorProcessor)
	 * @see #collection(Class opClass, Object input)
	 * @see #collection(Class opClass)
	 * 
	 * @param <OutputType> the {@link Operation Operation&lt;Void, OutputType&gt;}'s output type
	 *                      and {@link IteratorProcessor IteratorProcessor&lt;OutputType, ResultType&gt;}'s element type
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;Void, OutputType&gt;}
	 * @param <ResultType> the {@link IteratorProcessor IteratorProcessor&lt;OutputType, ResultType&gt;}'s result type
	 * @param opClass the concrete {@link Operation Operation&lt;Void, OutputType&gt;} implementation
	 * @param iteratorProcessor the {@link IteratorProcessor IteratorProcessor&lt;OutputType, ResultType&gt;} instance
	 * @return the {@code iteratorProcessor}'s result of type {@code ResultType}
	 */
	public <
		OutputType,
		OpClassType extends Operation<Void, OutputType>,
		ResultType
	>
	ResultType iterate(
		final Class<OpClassType> opClass,
		final IteratorProcessor<OutputType, ResultType> iteratorProcessor
	) {
		return iterate(opClass, null, iteratorProcessor);
	}
	
	/**
	 * Executes a database operation using the provided
	 * instance of {@code InputType} to fill in parameters,
	 * and returns a {@link Collection} of the resulting
	 * instances of {@code OutputType}.
	 * <p>
	 * If the database operation requires no input (meaning
	 * {@code <InputType} is {@link Void}) you must use
	 * {@link #collection(Class) collection(opClass)} instead,
	 * since {@link Void} is uninstantiable.
	 * 
	 * @see Operation
	 * @see #collection(Class)
	 * 
	 * @param <InputType> the {@link Operation Operation&lt;InputType, OutputType&gt;}'s input type
	 * @param <ConcreteInputType> the actual (concrete) implementation of {@code InputType}
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;InputType, OutputType&gt;}
	 * @param <OutputType> the {@link Operation Operation&lt;InputType, OutputType&gt;}'s output type
	 * @param opClass the concrete {@link Operation Operation&lt;InputType, OutputType&gt;} implementation
	 * @param input the {@code ConcreteInputType} instance used to fill in the operation's parameters
	 * @return the resulting {@link Collection Collection&lt;OutputType&gt;} of {@code OutputType} instances
	 */
	public <
		InputType,
		ConcreteInputType extends InputType,
		OutputType,
		OpClassType extends Operation<InputType, OutputType>
	>
	Collection<OutputType> collection(
		final Class<OpClassType> opClass,
		final ConcreteInputType input
	) {
		return iterate(opClass, input, new IteratorProcessor<OutputType, Collection<OutputType>>() {
			public Collection<OutputType> processIterator(Iterator<OutputType> iterator) {
				Collection<OutputType> collection = new java.util.LinkedList<OutputType>();
				while (iterator.hasNext())
					collection.add(iterator.next());
				return collection;
			}
			
		});
	}

	/**
	 * Executes a database operation which requires no input,
	 * and returns a {@link Collection} of the resulting
	 * instances of {@code OutputType}.
	 * <p>
	 * If the database operation requires input (meaning
	 * {@code <InputType} is not {@link Void}) you must use
	 * {@link #collection(Class, Object) collection(opClass, input)}
	 * instead.
	 * 
	 * @see Operation
	 * @see #collection(Class, Object)
	 * 
	 * @param <OutputType> the {@link Operation Operation&lt;Void, OutputType&gt;}'s output type
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;Void, OutputType&gt;}
	 * @param opClass the concrete {@link Operation Operation&lt;Void, OutputType&gt;} implementation
	 * @return the resulting {@link Collection Collection&lt;OutputType&gt;} of {@code OutputType} instances
	 */
	public <
		OutputType,
		OpClassType extends Operation<Void, OutputType>
	>
	Collection<OutputType> collection(
		final Class<OpClassType> opClass
	) {
		return collection(opClass, null);
	}

	/**
	 * Executes a single-row-returning database operation
	 * (flagged as such by implementing the interface
	 * {@link ReturnsSingleRow}) using the
	 * provided instance of {@code InputType} to fill in
	 * parameters, and returns a single {@code OutputType}
	 * instance representing the single result row, or
	 * {@code null} if no row was produced.
	 * <p>
	 * It is an error for an operation executed with
	 * this method to return more than one row. Doing
	 * so triggers an {@link IncorrectResultSizeDataAccessException}
	 * <p>
	 * If the database operation requires no input (meaning
	 * {@code <InputType} is {@link Void}) you must use
	 * {@link #get(Class) get(opClass)}
	 * instead, since {@link Void} is uninstantiable.
	 * 
	 * @see ReturnsSingleRow
	 * @see #get(Class opClass)
	 * 
	 * @param <InputType> the {@link Operation Operation&lt;InputType, OutputType&gt;}'s input type
	 * @param <ConcreteInputType> the actual (concrete) implementation of {@code InputType}
	 * @param <OutputType> the {@link Operation Operation&lt;InputType, OutputType&gt;}'s output type
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;InputType, OutputType&gt;}
	 * @param opClass the concrete {@link Operation Operation&lt;InputType, OutputType&gt;} implementation
	 * @param input the {@code ConcreteInputType} instance used to fill in the operation's parameters
	 * @return the resulting {@code OutputType} instance or {@code null}
	 */
	public <
		InputType,
		ConcreteInputType extends InputType,
		OutputType,
		OpClassType extends Operation<InputType, OutputType> & ReturnsSingleRow
	>
	OutputType get(
		final Class<OpClassType> opClass,
		final ConcreteInputType input
	) {
		return iterate(opClass, input, new IteratorProcessor<OutputType, OutputType>() {
			public OutputType processIterator(Iterator<OutputType> iterator) {
				if (!iterator.hasNext())
					return null;
				
				OutputType result = iterator.next();
				if (iterator.hasNext()) {
					throw new IncorrectResultSizeDataAccessException(
						"Statement " + opClass.getName() + " " +
						"was declared as " + ReturnsSingleRow.class.getSimpleName() + " " +
						"but returned more than one row",
						1,
						-1 /* Actual size unknown */
					);
				}
				
				return result;
			}
			
		});
	}
	
	/**
	 * Executes a single-row-returning database operation
	 * (flagged as such by implementing the interface
	 * {@link ReturnsSingleRow}) which requires
	 * no input and returns a single {@code OutputType}
	 * instance representing the single result row, or
	 * {@code null} if no row was produced.
	 * <p>
	 * It is an error for an operation executed with
	 * this method to return more than one row. Doing
	 * so triggers an {@link IncorrectResultSizeDataAccessException}
	 * <p>
	 * If the database operation requires input (meaning
	 * {@code <InputType} is not {@link Void}) you must use
	 * {@link #get(Class, Object) get(opClass, input)}
	 * instead.
	 * 
	 * @see ReturnsSingleRow
	 * @see #get(Class opClass, Object input)
	 * 
	 * @param <OutputType> the {@link Operation Operation&lt;Void, OutputType&gt;}'s output type
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;Void, OutputType&gt;}
	 * @param opClass the concrete {@link Operation Operation&lt;Void, OutputType&gt;} implementation
	 * @return the resulting {@code OutputType} instance or {@code null}
	 */
	public <
		OutputType,
		OpClassType extends Operation<Void, OutputType> & ReturnsSingleRow
	>
	OutputType get(
		final Class<OpClassType> opClass
	) {
		return get(opClass, null);
	}

	/**
	 * Executes a single-key-returning database operation
	 * (flagged as such by implementing the interfaces
	 * {@link ReturnsSingleRow}) and {@link GeneratesKey}
	 * while requires no input, and returns a single {@code KeyType}
	 * instance representing the generated key. The
	 * operation's {@code OutputType} must be {@link Void}.
	 * <p>
	 * It is an error for an operation executed with
	 * this method to return more than one key. Doing
	 * so triggers an {@link IncorrectResultSizeDataAccessException}
	 * <p>
	 * If the database operation requires input (meaning
	 * {@code <InputType} is not {@link Void}) you must use
	 * {@link #key(Class, Object) key(opClass, input)}
	 * instead.
	 * 
	 * @see ReturnsSingleRow
	 * @see GeneratesKey
	 * @see #key(Class opClass)
	 * 
	 * @param <InputType> the {@link Operation Operation&lt;InputType, OutputType&gt;}'s input type
	 * @param <ConcreteInputType> the actual (concrete) implementation of {@code InputType}
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;InputType, Void&gt;}
	 * @param <KeyType> the type of the generated key
	 * @param opClass the concrete {@link Operation Operation&lt;InputType, OutputType&gt;} implementation
	 * @param input the {@code ConcreteInputType} instance used to fill in the operation's parameters
	 * @return the resulting {@code KeyType} instance
	 */	
	public
	<
		InputType,
		ConcreteInputType extends InputType,
		KeyType extends Number,
		OpClassType extends Operation<InputType, Void> & GeneratesKey<KeyType> & ReturnsSingleRow
	>
	KeyType key(
		final Class<OpClassType> opClass,
		final ConcreteInputType input
	) {
		InputMapper<InputType> inputMapper = InputMapper.getInstance(
			opClass,
			(m_defaultInput != null) ? m_defaultInput.getClass() : null
		);

		KeyMapper<KeyType> keyMapper = KeyMapper.getInstance(opClass);
		KeyHolder keyHolder = new GeneratedKeyHolder();
		
		m_npJdbcTemplate.update(
			StatementMapper.getInstance(opClass).getStatement(input),
			inputMapper.mapInput(input, m_defaultInput),
			keyHolder,
			keyMapper.getGeneratedKeyColumns()
		);
		
		return keyMapper.mapKey(keyHolder);
	}

	/**
	 * Executes a single-key-returning database operation
	 * (flagged as such by implementing the interfaces
	 * {@link ReturnsSingleRow}) and {@link GeneratesKey}
	 * using the provided instance of {@code InputType} to fill in
	 * parameters, and returns a single {@code KeyType}
	 * instance representing the generated key. The
	 * operation's {@code OutputType} must be {@link Void}.
	 * <p>
	 * It is an error for an operation executed with
	 * this method to return more than one key. Doing
	 * so triggers an {@link IncorrectResultSizeDataAccessException}
	 * <p>
	 * If the database operation requires no input (meaning
	 * {@code <InputType} is {@link Void}) you must use
	 * {@link #get(Class) key(opClass)}
	 * instead, since {@link Void} is uninstantiable.
	 * 
	 * @see ReturnsSingleRow
	 * @see GeneratesKey
	 * @see #key(Class opClass, Object input)
	 * 
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;InputType, Void&gt;}
	 * @param <KeyType> the type of the generated key
	 * @param opClass the concrete {@link Operation Operation&lt;InputType, OutputType&gt;} implementation
	 * @return the resulting {@code KeyType} instance
	 */	
	public
	<
		KeyType extends Number,
		OpClassType extends Operation<Void, Void> & GeneratesKey<KeyType> & ReturnsSingleRow
	>
	KeyType key(
		final Class<OpClassType> opClass
	) {
		return key(opClass, null);
	}

	/**
	 * Executes a database operation which produces no output,
	 * using the provided instance of {@code InputType} to fill
	 * in parameters
	 * <p>
	 * If the database operation requires no input (meaning
	 * {@code <InputType} is {@link Void}) you must use
	 * {@link #execute(Class) execute(opClass)}
	 * instead, since {@link Void} is uninstantiable.
	 * 
	 * @param <InputType> the {@link Operation Operation&lt;InputType, Void&gt;}'s input type
	 * @param <ConcreteInputType> the actual (concrete) implementation of {@code InputType}
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;InputType, Void&gt;}
	 * @param opClass the concrete {@link Operation Operation&lt;InputType, Void&gt;} implementation
	 * @param input the {@code ConcreteInputType} instance used to fill in the operation's parameters
	 */
	public <
		InputType,
		ConcreteInputType extends InputType,
		OpClassType extends Operation<InputType, Void>
	>
	void execute(
		final Class<OpClassType> opClass,
		final ConcreteInputType input
	) {
		InputMapper<InputType> inputMapper = InputMapper.getInstance(
			opClass,
			(m_defaultInput != null) ? m_defaultInput.getClass() : null
		);

		m_npJdbcTemplate.update(
			StatementMapper.getInstance(opClass).getStatement(input),
			inputMapper.mapInput(input, m_defaultInput)
		);
	}
	
	/**
	 * Executes a database operation which produces no output
	 * and requires no input.
	 * <p>
	 * If the database operation requires input (meaning
	 * {@code <InputType} is {@link Void}) you must use
	 * {@link #execute(Class, Object) execute(opClass, input)}
	 * instead.
	 * 
	 * @param <OpClassType> the operation class'es actual type, extends {@link Operation Operation&lt;Void, Void&gt;}
	 * @param opClass the concrete {@link Operation Operation&lt;Void, Void&gt;} implementation
	 */
	public <
		OpClassType extends Operation<Void, Void>
	>
	void execute(
		final Class<OpClassType> opClass
	) {
		execute(opClass, null);
	}

}

