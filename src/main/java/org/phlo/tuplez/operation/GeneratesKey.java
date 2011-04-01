package org.phlo.tuplez.operation;

/**
 * Marks implementation of {@link Operation} as
 * generating key values of type {@literal KeyType}.
 * Usually used on operations which represent INSERT
 * statements. 
 * <p>
 * This is required to execute such operations with
 * {@link org.phlo.tuplez.Executor#key(Class, Object) Executor(opClass, input)}
 * or {@link org.phlo.tuplez.Executor#key(Class opClass) Executor(opClass)},
 * which return the generated key.
 * <p>
 * The column containing the generated key can be
 * specified by annotating the concrete {@link Operation}
 * instance with {@link KeyColumn}.
 * <p>
 * This interface defines no methods, it simply serves as
 * a way of distinguishing at compile-time between
 * {@link Operation}s which return generate keys and such
 * with don't.
 * 
 * @see org.phlo.tuplez.Executor#key(Class, Object) Executor(opClass, input)
 * @see org.phlo.tuplez.Executor#key(Class opClass) Executor(opClass)
 * 
 * @param <KeyType> the generated key's type. Must be one of
 *                  {@link Short}, {@link Integer}, {@link Long}
 *                  {@link java.math.BigInteger BigInteger},
 *                  {@link java.math.BigDecimal BigDecimal}
 */

public interface GeneratesKey<KeyType extends Number> extends OperationOutput<Void> {
	/* Just a marker, no members */
}
