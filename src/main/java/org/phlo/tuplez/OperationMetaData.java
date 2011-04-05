package org.phlo.tuplez;

import org.phlo.tuplez.operation.*;

import com.googlecode.gentyref.GenericTypeReflector;

public final class OperationMetaData {
	private OperationMetaData() {};
	
	public static <
		InputType,
		OpClassType extends Operation<InputType, ?> & OperationInput<InputType>
	>
	Class<InputType>
	getOperationInputClass(final Class<OpClassType> opClass) {
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
		OpClassType extends Operation<?, OutputType> & OperationOutput<OutputType>
	>
	Class<OutputType>
	getOperationOutputClass(final Class<OpClassType> opClass) {
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
		OpClassType extends Operation<?, Void> & GeneratesKey<KeyType>
	>
	Class<KeyType>
	getOperationKeyClass(final Class<OpClassType> opClass) {
		@SuppressWarnings("unchecked")
		Class<KeyType> keyClass = (Class<KeyType>)GenericTypeReflector.getTypeParameter(
			opClass,
			GeneratesKey.class.getTypeParameters()[0]
		);
		if (keyClass == null)
			throw new InvalidOperationDefinitionException("unable to determine key type", opClass);
		
		return keyClass;
	}
}
