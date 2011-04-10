package org.phlo.tuplez;

import java.util.*;
import java.sql.*;

import org.springframework.dao.*;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.core.namedparam.*;
import org.springframework.jdbc.support.*;

import org.phlo.tuplez.operation.*;

@SuppressWarnings({"rawtypes", "unchecked"})
class OperationDefaultImplementation implements
	Cloneable,
	Operation,
	OperationGeneratesKey,
	OperationReturnsSingleRow
{
	final private Class<? extends Operation<?,?>> m_opClass;

	Object m_operation = this;
	
	final private NamedParameterJdbcTemplate m_npJdbcTemplate;

	final private Object m_defaultInput;

	final private InputMapper m_inputMapper;
	
	final private KeyMapper m_keyMapper;
	
	final private String m_statementStatic;

	boolean m_inputSet = false;

	private Object m_input;
	
	/**
	 * Used only to generate the <b>prototype</b>
	 * operation instance of a concrete operation
	 * in {@link OperationFactory}
	 */
	OperationDefaultImplementation() {
		m_opClass = null;
		m_npJdbcTemplate = null;
		m_defaultInput = null;
		m_inputMapper = null;
		m_keyMapper = null;
		m_statementStatic = null;
	}
	
	/**
	 * Create a new {@link OperationDefaultImplementation}
	 * for a concrete operation definition (class or interface)
	 * which uses the provide {@link NamedParameterJdbcTemplate}
	 * to execute queries and the provided default input object
	 * to fill in default.* parameters.
	 * 
	 * @param opClass concrete operation definition
	 * @param npJdbcTemplate named-parameter JDBC template used to execute queries
	 * @param defaultInput statement default parameter source
	 */
	OperationDefaultImplementation(
		final Class<? extends Operation<?,?>> opClass,
		final NamedParameterJdbcTemplate npJdbcTemplate,
		final Object defaultInput
	) {
		m_opClass = opClass;
		m_npJdbcTemplate = npJdbcTemplate;
		m_defaultInput = defaultInput;

		m_inputMapper = InputMapper.getInstance(
			(Class<? extends Operation<Object,?>>)getOperationClass(),
			(m_defaultInput != null) ? m_defaultInput.getClass() : Void.class
		);
		
		if (OperationGeneratesKey.class.isAssignableFrom(opClass))
			m_keyMapper = KeyMapper.getInstance((Class<OperationGeneratesKey>)opClass);
		else
			m_keyMapper = null;

		
		m_statementStatic = OperationMetaData.getStatementStatic(opClass);
	}
	
	public OperationDefaultImplementation clone() {
		try {
			return (OperationDefaultImplementation)super.clone();
		}
		catch (CloneNotSupportedException e) {
			throw new RuntimeException("clone() failed unexpectedly", e);
		}
	}
	
	/**
	 * Represents a block of code.
	 * 
	 * @param <T> the block's return type
	 */
	private static interface Block<T> {
		T block();
	}
	
	/**
	 * Executes the given block while the operation's input is set
	 * to the given value. The old state is restored afterwards
	 * 
	 * @param <T> return type of block
	 * @param input input to set temporarily
	 * @param block the block to execute
	 * @return the block's return value
	 */
	private <T> T withInput(final Object input, final Block<T> block) {
		final OperationInput op = _getActualImplementation(OperationInput.class);

		boolean previousInputSet = op.getInputSet();
		Object previousInput = null;
		if (previousInputSet)
			previousInput = op.getInput();

		try {
			op.setInput(input);
			return block.block();
		}
		finally {
			if (previousInputSet)
				op.setInput(previousInput);
			else
				op.clearInput();
		}
	}
	
	@Override
	public Class<? extends Operation> getOperationClass() {
		return m_opClass;
	}

	@Override
	public Object getActualImplementation(final Class ifaceClass) {
		return _getActualImplementation(ifaceClass);
	}
	
	/**
	 * Generic version of {@link #getActualImplementation(Class)}
	 * <p>
	 * OperationDefaultImplementation <b>isn't</b> always
	 * a superclass of the operation instances returned by
	 * {@link OperationFactory}. Therefore, members of 
	 * {@link Operation} and friends that are overriden
	 * in an operation's definition aren't accessible from
	 * default implementation methods. To work around that,
	 * other {@link Operation} members <b>must</b> always be
	 * called with
	 * <pre>{@literal getActualImplementation(<DefiningInteface>.class).<Member>(...)}</pre>
	 * <p>
	 * This method has the same erasure as
	 * {@link Operation#getActualImplementation(Class)},
	 * but the compiler still doesn't accept it as
	 * that methods implementation. Probably because
	 * this class inherits the <b>raw</b> type
	 * {@link Operation}, and that causes all generic
	 * signatures to be ignored, even those independent
	 * from the interface's type parameters. Thus, it's
	 * declared as a seperate method that
	 * {@link #getActualImplementation(Class)} delegates
	 * to, thereby allowing other methods in this class
	 * to use the generic version of the method while
	 * still providing an implemetation of the {@link Operation}
	 * interface.
	 * 
	 * @see #getActualImplementation(Class)
	 * 
	 * @param <IfaceType> interface type
	 * @param ifaceClass interface to get the actual implementation of
	 * @return the interface's default implementation
	 */
	private <IfaceType> IfaceType _getActualImplementation(final Class<IfaceType> ifaceClass) {
		if (!ifaceClass.isInstance(m_operation))
			throw new ClassCastException("operation instance does not support the interface " + ifaceClass);
		return (IfaceType)m_operation;
	}

	@Override
	public Object getDefaultImplementation(final Class ifaceClass) {
		if (!ifaceClass.isInstance(this))
			throw new ClassCastException("no default implementation for interface " + ifaceClass);

		return this;
	}

	@Override
	public boolean getInputSet() {
		return m_inputSet;
	}
	
	@Override
	public Object getInput() {
		if (m_inputMapper.isInputVoid())
			return null;
		
		if (!m_inputSet) {
			throw new InvalidOperationExecutionException(
				"no input set for operation instance with non-void input type",
				_getActualImplementation(Operation.class).getOperationClass()
			);
		}
		
		return m_input;
	}
	
	@Override
	public void setInput(Object input) {
		if (m_inputMapper.isInputVoid()) {
			throw new InvalidOperationExecutionException(
				"cannot set input for operation instance with void input type",
				_getActualImplementation(Operation.class).getOperationClass()
			);
		}
		
		m_inputSet = true;
		m_input = input;
	}
	
	@Override
	public void clearInput() {
		m_inputSet = false;
		m_input = null;
	}
	
	@Override
	public String getStatement() {
		OperationStatementIsComputed opStmtComputed;
		try {
			opStmtComputed = _getActualImplementation(OperationStatementIsComputed.class);
		}
		catch (ClassCastException e) {
			return m_statementStatic;
		}

		return opStmtComputed.getStatement(getInput());
	}
	
	@Override
	public Object iterate(final Object input, final IteratorProcessor iteratorProcessor) {
		return withInput(input, new Block<Object>() {
			@Override public Object block() { return iterate(iteratorProcessor); }
		});
	}

	@Override
	public Object iterate(final IteratorProcessor iteratorProcessor) {
		final Operation op = _getActualImplementation(Operation.class);
		
		final SqlParameterSource inputSrc;
		if (m_inputMapper.isInputVoid())
			inputSrc = m_inputMapper.mapInput(m_defaultInput);
		else
			inputSrc = m_inputMapper.mapInput(op.getInput(), m_defaultInput);
			
		return m_npJdbcTemplate.query(
			op.getStatement(),
			inputSrc,
			new ResultSetExtractor() {
				public Object extractData(final ResultSet resultSet) throws SQLException {
					ResultSetMapper rsMapper = ResultSetMapper.getInstance(op.getOperationClass(), resultSet);
					
					try {
						return iteratorProcessor.processIterator(new ResultSetIterator(resultSet, rsMapper));
					}
					catch (WrappedSQLException e) {
						throw e.getSQLException();
					}
				}
			}
		);
	}

	@Override
	public Collection collection(Object input) {
		final Operation op = _getActualImplementation(Operation.class);

		return withInput(input, new Block<Collection>() {
			@Override public Collection block() { return op.collection(); }
		});
	}

	@Override
	public Collection collection() {
		final Operation<Object,Object> op = _getActualImplementation(Operation.class);

		return op.iterate(new IteratorProcessor<Object, Collection>() {
			public Collection processIterator(Iterator iterator) {
				Collection collection = new java.util.LinkedList();
				while (iterator.hasNext())
					collection.add(iterator.next());
				return collection;
			}
			
		});
	}

	@Override
	public Object get(Object input) {
		final OperationReturnsSingleRow op = _getActualImplementation(OperationReturnsSingleRow.class);

		return withInput(input, new Block<Object>() {
			@Override public Object block() { return op.get(); }
		});
	}

	@Override
	public Object get() {
		final OperationReturnsSingleRow op = _getActualImplementation(OperationReturnsSingleRow.class);

		return op.iterate(new IteratorProcessor() {
			public Object processIterator(Iterator iterator) {
				if (!iterator.hasNext())
					return null;
				
				Object result = iterator.next();
				if (iterator.hasNext()) {
					throw new IncorrectResultSizeDataAccessException(
						"Statement " + op.getOperationClass().getName() + " " +
						"was declared as " + OperationReturnsSingleRow.class.getSimpleName() + " " +
						"but returned more than one row",
						1,
						-1 /* Actual size unknown */
					);
				}
				
				return result;
			}
			
		});
	}

	@Override
	public Number key(Object input) {
		return withInput(input, new Block<Number>() {
			@Override public Number block() { return key(); }
		});
	}

	@Override
	public Number key() {
		final OperationGeneratesKey op = _getActualImplementation(OperationGeneratesKey.class);

		final SqlParameterSource inputSrc;
		if (m_inputMapper.isInputVoid())
			inputSrc = m_inputMapper.mapInput(m_defaultInput);
		else
			inputSrc = m_inputMapper.mapInput(op.getInput(), m_defaultInput);

		final KeyHolder keyHolder = new GeneratedKeyHolder();
		
		m_npJdbcTemplate.update(
			op.getStatement(),
			inputSrc,
			keyHolder,
			m_keyMapper.getGeneratedKeyColumns()
		);
		
		return m_keyMapper.mapKey(keyHolder);
	}

	@Override
	public void execute(Object input) {
		final Operation op = _getActualImplementation(Operation.class);

		withInput(input, new Block<Void>() {
			@Override public Void block() { op.execute(); return null; }
		});
	}

	@Override
	public void execute() {
		final Operation op = _getActualImplementation(Operation.class);

		final SqlParameterSource inputSrc;
		if (m_inputMapper.isInputVoid())
			inputSrc = m_inputMapper.mapInput(m_defaultInput);
		else
			inputSrc = m_inputMapper.mapInput(op.getInput(), m_defaultInput);

		m_npJdbcTemplate.update(
			op.getStatement(),
			inputSrc
		);
	}
}
