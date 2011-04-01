package org.phlo.tuplez.operation;

/**
 * Marks implementation of {@link Operation} as having
 * a SQL statement which depends on the input parameter an
 * can therefore not be specified by an annotation.
 * <p>
 * Implementations of {@link Operation} must provide a
 * {@link #getStatement(Object) getStatement(InputType input)} method.
 * <p>
 * The type passed as InputType here must be assignable from
 * instances of the database operation's InputType.
 * 
 * @see #getStatement(Object)
 *
 * @param <InputType> Interface representing a database operation's input parameters
 */
public interface StatementIsComputed<InputType> extends OperationInput<InputType> {
	/**
	 * Must return the statement to be used to execute an 
	 * {@link StatementIsComputed} implementation
	 * <p>
	 * The returned statement can use parameter references to
	 * reference input values. The only substitutions which
	 * are supposed to be done in this method are ones for which
	 * normal parameter substitution is not sufficient. One
	 * could, for example, add different "ORDER BY" clauses to
	 * the statement depending on some input value.
	 * <p>
	 * @param input the operation's input values
	 * @return the statement to be sent to the database.
	 */
	String getStatement(final InputType input);
}
