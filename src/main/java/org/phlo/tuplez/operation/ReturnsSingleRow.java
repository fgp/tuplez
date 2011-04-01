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
public interface ReturnsSingleRow {
	/* Just a marker, no members */
}
