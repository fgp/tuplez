package org.phlo.tuplez.operation;

/**
 * Marks implementation of {@link Operation} as
 * single-row returning.
 * <p>
 * This is required to execute such operations with
 * {@link org.phlo.tuplez.Executor#get(Class, Object) Executor#get(opClass, input)}
 * or {@link org.phlo.tuplez.Executor#get(Class opClass) Executor#get(opClass)}
 * <p>
 * This interface defines no methods, it simply serves as
 * a way of distinguishing at compile-time bewteen
 * {@link Operation}s which returns at most one row,
 * and such which possibly more.
 * 
 * @see org.phlo.tuplez.Executor#get(Class, Object) Executor#get(opClass, input)
 * @see org.phlo.tuplez.Executor#get(Class opClass) Executor#get(opClass)
 */
public interface OperationReturnsSingleRow<InputType, OutputType> extends Operation<InputType, OutputType> {
	/**
	 * Executes the operation, returning the single
	 * output produced or null.
	 * 
	 * @see org.phlo.tuplez.Executor#get(Class, Object)
	 * 
	 * @param input the operation's input
	 * @return the output produced or null
	 */
	OutputType get(final InputType input);
	
	/**
	 * Executes the operation, returning the single
	 * output produced or null.
	 * 
	 * @see org.phlo.tuplez.Executor#get(Class)
	 * 
	 * @return the output produced or null
	 */
	OutputType get();
}
