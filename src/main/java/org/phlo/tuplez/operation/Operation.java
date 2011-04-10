package org.phlo.tuplez.operation;

import java.util.Collection;

import org.phlo.tuplez.IteratorProcessor;

/**
 * {@link Operation} is base Interface which must
 * be implemented by all classes representing concrete
 * database operations (i.e. SQL statements).
 * <p>
 * The SQL statement represented by an {@link Operation}
 * can either be supplied by annotating the concrete
 * Operation implementation with
 * {@link org.phlo.tuplez.operation.Statement}, or by
 * implementing the interface {@link StatementIsComputed}
 * in addition to {@link Operation}.
 * <p>
 * For each (named) parameter in the SQL statement, the InputType
 * must define a corresponding getter method (following
 * the usual Java Bean naming convention, i.e. the getter
 * for the parameter :myValue has to be called getMyValue()).
 * <p>
 * Similarly, for each column of the statement's result,
 * the OutputType must contain a getter. The name to column
 * matching is more lenient than in the input case - the
 * getter's name must get{@literal <columnName>} after both
 * strings have been down-cased and all non-alphanumeric
 * characters have been removed.
 * <p>
 * The OutputType's getter's return type determined the
 * JDBC ResultSet accessor method used to extract the output
 * value.
 * <p>
 * The mapping of column names to OutputType getters and
 * to ResultSet accessors can be overriden by annotating
 * concrete database operation classes with
 * {@link org.phlo.tuplez.operation.OutputMapping}.
 * <p>
 * {@link Operation} extends {@link OperationInput} and
 * {@link OperationOutput}, each parameterized with the
 * {@literal InputType} respectively the {@literal OutputType}.
 * This allows other parameterized interfaces like
 * {@link OperationGeneratesKey} to force a particular
 * {@literal InputType} or {@literal OutputType}.
 * 
 * @see OperationInput
 * @see OperationOutput
 * @see org.phlo.tuplez.operation.Statement
 * @see org.phlo.tuplez.operation.OutputMapping
 *
 * @param <InputType> Interface representing a database operation's input parameters
 * @param <OutputType> Interface representing a database operation's output parameters
 */
public interface Operation<InputType, OutputType> extends OperationInput<InputType>, OperationOutput<OutputType> {	
	Class<? extends Operation<InputType, OutputType>> getOperationClass();
	
	<IfaceType> IfaceType getDefaultImplementation(final Class<IfaceType> ifaceClass);

	<Iface> Iface getActualImplementation(final Class<Iface> ifaceClass);

	String getStatement();
	
	/**
	 * Executes the operation, passing an iterator over the
	 * output to an {@link IteratorProcessor}
	 * 
	 * @see org.phlo.tuplez.Executor#iterate(Class, Object, IteratorProcessor)
	 * 
	 * @param <ResultType> the iterator processor's result type
	 * @param input the operation's input 
	 * @param iteratorProcessor the iterator processor
	 * @return the result produces by the iterator processor
	 */
	<ResultType> ResultType iterate(final InputType input, final IteratorProcessor<OutputType, ResultType> iteratorProcessor);
	
	/**
	 * Executes the operation, passing an iterator over the
	 * output to an {@link IteratorProcessor}
	 * 
	 * @see org.phlo.tuplez.Executor#iterate(Class, IteratorProcessor)
	 * 
	 * @param <ResultType> the iterator processor's result type
	 * @param iteratorProcessor the iterator processor
	 * @return the result produces by the iterator processor
	 */
	<ResultType> ResultType iterate(final IteratorProcessor<OutputType, ResultType> iteratorProcessor);

	/**
	 * Executes the operation, returning a collection containing
	 * the output.
	 * 
	 * @see org.phlo.tuplez.Executor#collection(Class, Object)
	 * 
	 * @param input the operation's input
	 * @return collection over the statement's output
	 */
	Collection<OutputType> collection(final InputType input);
	
	/**
	 * Executes the operation, returning a collection containing
	 * the output.
	 * 
	 * @see org.phlo.tuplez.Executor#collection(Class)
	 * 
	 * @return collection over the statement's output
	 */
	Collection<OutputType> collection();

	/**
	 * Executes the operation
	 * 
	 * @see org.phlo.tuplez.Executor#execute(Class, Object)
	 * @param input the operation's input
	 */
	void execute(final InputType input);

	/**
	 * Executes the operation
	 * 
	 * @see org.phlo.tuplez.Executor#execute(Class)
	 */
	void execute();
}