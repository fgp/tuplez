package org.phlo.tuplez;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import org.phlo.tuplez.operation.*;

/**
 * Provides support for mapping objects of type InputType
 * to instances of {@link SqlParameterSource}.
 * 
 * The parameter source returned by mapInput() supports
 * three different classes of parameters, distinguished
 * by their prefix
 * <ul>
 * <li><b>in.*</b> These are mapped to methods named
 *     <b>get*</b> of the InputType. Standard Java Beans naming
 *     conventions apply - i.e, in.id is mapped to InputType.getId()
 * <li><b>in</b> Mapped to the instance of InputType
 *     itself. Mainly useful InputType is some JDBC-supported
 *     type like {@link String} or {@link Integer}.
 * <li><b>default.*</b> Mapped to methods of the executor
 *      used to execute the statement. Again, standard Java Beans naming
 *      conventions apply. Mainly useful for injecting global (or
 *      per-request) parameters into the query, i.e. parameter reflecting
 *      the permissions granted to the current user.
 * </ul>
 * @author fgp
 *
 * @param <InputType>
 */
public final class InputMapper<InputType> {
	/**
	 * Input mapper instance cache
	 */
	private static final ConcurrentMap<ID<?>, InputMapper<?>> s_inputMappers =
		new java.util.concurrent.ConcurrentHashMap<ID<?>, InputMapper<?>>();

	/**
	 * Encapsulates the identifying parameters of a ResultSetMapper instance.
	 */
	static private final class ID<InputType> {
		final public Class<? extends Operation<InputType, ?>> opClass;
		final private Class<?> defaultInputClass;
		
		ID(
			final Class<? extends Operation<InputType, ?>> _opClass,
			final Class<?> _defaultInputClass
		) {
			assert _opClass != null;
			assert _defaultInputClass != null;
			
			opClass = _opClass;
			defaultInputClass = _defaultInputClass;
		}
			
		@Override
		public int hashCode() {
			return
			opClass.hashCode() ^
			defaultInputClass.hashCode();
		}

		@Override
		public boolean equals(Object other) {
			if (this == other)
				return true;
			if (!(other instanceof ID))
				return false;
			
			ID<?> otherID = (ID<?>)other;
			if (!opClass.equals(otherID.opClass))
				return false;
			if (!defaultInputClass.equals(otherID.defaultInputClass))
				return false;
			return true;
		}
	}
	
	/**
	 * Factory methods for InputMapper instances
	 * 
	 * @param <InputType> InputType of the {@link Operation}
	 * @param opClass The concrete {@link Operation}
	 * @return An InputMapper instance
	 */
	public static <InputType> InputMapper<InputType> getInstance(
		final Class<? extends Operation<InputType, ?>> opClass,
		final Class<?> defaultInputClass
	) {
		ID<InputType> imID = new ID<InputType>(opClass, defaultInputClass);
		
		@SuppressWarnings("unchecked")
		InputMapper<InputType> inputMapper = (InputMapper<InputType>)s_inputMappers.get(imID);
		if (inputMapper == null) {
			inputMapper = new InputMapper<InputType>(opClass, defaultInputClass);
			s_inputMappers.put(imID, inputMapper);
		}
		
		return inputMapper;
	}
	
	/**
	 * Parameter scope.
	 * 
	 * DEFAULT comes from the executor's default input source,
	 * IN comes from the input
	 * IN_WHOLE represents the *whole* input (i.e. not only a single field)
	 */
	private enum Scope {DEFAULT, IN, IN_WHOLE};
	
	/**
	 * Per-field meta data
	 */
	private static final class FieldMetaData {
		Scope scope;
		Class<?> fieldClass;
		Method getterMethod;
		int jdbcSqlType;
		boolean transformToString;
	}
	
	/**
	 * The operation's class
	 */
	private final Class<? extends Operation<InputType, ?>> m_opClass;

	/**
	 * The available input fields, indexed by their parameter name
	 * (i.e. in.someField, in, default.defaultField, ...)
	 */
	private final Map<String, FieldMetaData> m_fields =
		new java.util.HashMap<String, FieldMetaData>();
	

	/**
	 * Constructs an InputMapper instance for a concrete {@link Operation}
	 * 
	 * @param opClass the class object of the concrete {@link Operation}
	 */
	private InputMapper(
		final Class<? extends Operation<InputType, ?>> opClass,
		final Class<?> defaultInputClass
	) {
		m_opClass = opClass;
		
		/* Get InputType of statement class */
		Class<InputType> inputClass = OperationMetaData.getOperationInputClass(opClass);
		
		/* Scan default input for getters */
		for(Method defaultMethod: defaultInputClass.getMethods())
			addField(Scope.DEFAULT, defaultMethod);
		
		/* Scan input for getters */
		for(Method inputMethod: inputClass.getMethods())
			addField(Scope.IN, inputMethod);
		
		/* Add special field "in" representing the whole input */
		FieldMetaData inWholeMetaData = new FieldMetaData();
		inWholeMetaData.scope = Scope.IN_WHOLE;
		inWholeMetaData.fieldClass = inputClass;
		computeTypes(inWholeMetaData);
		m_fields.put("in", inWholeMetaData);
	}
	
	public boolean addField(Scope scope, Method method) {
		assert (scope == Scope.DEFAULT) || (scope == Scope.IN);
		
		/* Accessors mapped to fields must be public non-static members */
		int methodModifiers = method.getModifiers();
		if (!Modifier.isPublic(methodModifiers) || Modifier.isStatic(methodModifiers))
			return false;
		
		/* Their name must be "get*" */
		String methodName = method.getName();
		if ((methodName.length() <= 3) || !methodName.startsWith("get"))
			return false;

		/* And they must take zero parameters */
		if (method.getParameterTypes().length > 0)
			return false;
		
		/* Compute field name and create FieldMetaData instance */

		String fieldName =
			scope.toString().toLowerCase() + "." +
			methodName.substring(3, 4).toLowerCase() +
			methodName.substring(4);
		
		FieldMetaData fieldMeta = new FieldMetaData();
		
		fieldMeta.scope = scope;
		fieldMeta.getterMethod = method;
		fieldMeta.fieldClass = method.getReturnType();
		
		/* Compute SQL type */
		computeTypes(fieldMeta);
		
		/* And finally register field */
		m_fields.put(fieldName, fieldMeta);
		
		return true;
	}
	
	public void computeTypes(FieldMetaData fieldMeta) {
		/* We use jdbcTemplate's default mapping from Java types
		 * to SQL types except for Enums, which we special-case
		 * by converting them to strings
		 */
		
		if (fieldMeta.fieldClass.isEnum()) {
			fieldMeta.jdbcSqlType = java.sql.Types.VARCHAR;
			fieldMeta.transformToString = true;
		}
		else {
			fieldMeta.jdbcSqlType = StatementCreatorUtils.javaTypeToSqlParameterType(fieldMeta.fieldClass);
		}
	}
	
	public SqlParameterSource mapInput(final InputType input, final Object defaultInput) {
		return new SqlParameterSource() {
			@Override
			public boolean hasValue(String paramName) {
				return m_fields.containsKey(paramName);
			}

			@Override
			public Object getValue(String paramName) throws IllegalArgumentException {
				/* Complain if the field is not defined */
				FieldMetaData fieldMeta = m_fields.get(paramName);
				if (fieldMeta == null)
					throw new IllegalArgumentException("No value for parameter " + paramName);
				
				/* Get field value */
				Object value;
				try {
					switch (fieldMeta.scope) {
						case IN_WHOLE:
							value = input;
							break;
							
						case IN:
							if (input == null) {
								throw new InvalidOperationExecutionException(
									"operation uses field " + paramName + ", " +
									"but the whole input was null",
									m_opClass
								);
							}
							value = fieldMeta.getterMethod.invoke(input);
							break;
						
						case DEFAULT:
							if (defaultInput == null) {
								throw new InvalidOperationExecutionException(
									"operation uses default input but none was set",
									m_opClass
								);
							}
							value = fieldMeta.getterMethod.invoke(defaultInput);
							break;
						
						default:
							throw new RuntimeException("invalid parameter scope " + fieldMeta.scope);
					}
				}
				catch (IllegalAccessException e) {
					throw new InvalidOperationDefinitionException(
						"Getter for input parameter " + paramName + " " +
						"could not be invoked",
						m_opClass,
						e
					);
				}
				catch (InvocationTargetException e) {
					if (e.getTargetException() instanceof RuntimeException)
						throw (RuntimeException)e.getTargetException();
					else
						throw new InvalidOperationDefinitionException(
							"Getter for input parameter " + paramName + " " +
							"failed",
							m_opClass,
							e
						);
				}
				
				/* Apply post-processing if necessary */

				if (fieldMeta.transformToString && (value != null))
					value = value.toString();
				
				return value;
			}

			@Override
			public int getSqlType(String paramName) {
				FieldMetaData fieldMeta = m_fields.get(paramName);
				if (fieldMeta != null)
					return fieldMeta.jdbcSqlType;
				else
					return TYPE_UNKNOWN;
			}

			@Override
			public String getTypeName(String paramName) {
				/* Don't know what to return here, and it
				 * doesn't seem to be called anyway...
				 */
				return null;
			}
		};
	}
}
