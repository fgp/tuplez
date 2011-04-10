package org.phlo.tuplez.operation;

import com.googlecode.gentyref.GenericTypeReflector;

import org.phlo.tuplez.InvalidOperationDefinitionException;

/**
 * {@link OperationMetaData} is only a container
 * for static helper methods, creating instances
 * of this class is meaningless
 */
public final class OperationMetaData {
	private OperationMetaData() {};
	
	public static <
		InputType,
		OpType extends Operation<InputType, ?> & OperationInput<InputType>
	>
	Class<InputType>
	getInputClass(final Class<OpType> opClass) {
		@SuppressWarnings("unchecked")
		Class<InputType> inputClass = (Class<InputType>)GenericTypeReflector.getTypeParameter(
			opClass,
			OperationInput.class.getTypeParameters()[0]
		);
		if (inputClass == null)
			throw new InvalidOperationDefinitionException("unable to determine input type", opClass);
		
		return inputClass;
	}
	
	public static <
		OutputType,
		OpType extends Operation<?, OutputType> & OperationOutput<OutputType>
	>
	Class<OutputType>
	getOutputClass(final Class<OpType> opClass) {
		@SuppressWarnings("unchecked")
		Class<OutputType> outputClass = (Class<OutputType>)GenericTypeReflector.getTypeParameter(
			opClass,
			OperationOutput.class.getTypeParameters()[0]
		);
		if (outputClass == null)
			throw new InvalidOperationDefinitionException("unable to determine output type", opClass);
		
		return outputClass;
	}

	public static <
		KeyType extends Number,
		InputType,
		OpType extends Operation<InputType, Void> & OperationGeneratesKey<InputType, KeyType>
	>
	Class<KeyType>
	getKeyClass(final Class<OpType> opClass) {
		@SuppressWarnings("unchecked")
		Class<KeyType> keyClass = (Class<KeyType>)GenericTypeReflector.getTypeParameter(
			opClass,
			OperationKey.class.getTypeParameters()[0]
		);
		if (keyClass == null)
			throw new InvalidOperationDefinitionException("unable to determine key type", opClass);
		
		return keyClass;
	}
	
	public static <
		InputType,
		OutputType,
		OpType extends Operation<InputType,OutputType>
	>
	String getStatementStatic(
		final Class<OpType> opClass
	) {
		Statement atStmt = opClass.getAnnotation(Statement.class);
		if (atStmt != null)
			return atStmt.value();
		else
			return null;
	}
}
