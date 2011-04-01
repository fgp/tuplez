package org.phlo.tuplez;

import java.util.concurrent.ConcurrentMap;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.googlecode.gentyref.GenericTypeReflector;

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
 *     itself. Mainly usefullif InputType is some JDBC-supported
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
	 * Scope of a certain parameter
	 */
	private enum Scope {DEFAULT, IN_PROPERTY, IN_WHOLE};

	/**
	 * Input mapper instance cache
	 */
	private static final ConcurrentMap<Class<? extends Operation<?, ?>>, InputMapper<?>> s_inputMappers = new java.util.concurrent.ConcurrentHashMap<Class<? extends Operation<?, ?>>, InputMapper<?>>();

	/**
	 * Factory methods for InputMapper instances
	 * 
	 * @param <InputType> InputType of the {@link Operation}
	 * @param opClass The concrete {@link Operation}
	 * @return An InputMapper instance
	 */
	public static <InputType> InputMapper<InputType> getInstance(final Class<? extends Operation<InputType, ?>> opClass) {
		@SuppressWarnings("unchecked")
		InputMapper<InputType> inputMapper = (InputMapper<InputType>)s_inputMappers.get(opClass);
		if (inputMapper == null) {
			inputMapper = new InputMapper<InputType>(opClass);
			s_inputMappers.put(opClass, inputMapper);
		}
		
		return inputMapper;
	}
	
	private Class<?> m_inputClass;
	
	/**
	 * Constructs an InputMapper instance for a concrete {@link Operation}
	 * 
	 * @param opClass the class object of the concrete {@link Operation}
	 */
	private InputMapper(final Class<? extends Operation<InputType, ?>> opClass) {
		/* Get InputType of statement class */
		m_inputClass = (Class<?>)GenericTypeReflector.getTypeParameter(
			opClass,
			OperationInput.class.getTypeParameters()[0]
		);
		if (m_inputClass == null)
			throw new InvalidDataAccessApiUsageException("Failed to determine statement's input class");
	}
	
	public SqlParameterSource mapInput(final Executor executor, final InputType input) {
		final SqlParameterSource defaultSqlParameterSource = executor.getDefaultInputSqlParameterSource();
		final SqlParameterSource inputSqlParameterSource = (input == null ? null : new BeanPropertySqlParameterSource(input));
		
		return new SqlParameterSource() {
			private Scope getScope(String paramName) {
				if (paramName.startsWith("default."))
					return Scope.DEFAULT;
				else if (paramName.startsWith("in."))
					return Scope.IN_PROPERTY;
				else if (paramName.equals("in"))
					return Scope.IN_WHOLE;

				return null;
			}
			
			private String getSourceParameterName(Scope scope, String paramName) {
				switch (scope) {
					case DEFAULT:return paramName.substring(8);
					case IN_PROPERTY: return paramName.substring(3);
				}
				
				return null;
			}
			
			public boolean hasValue(String paramName) {
				final Scope scope = getScope(paramName);
				
				switch (scope) {
					case DEFAULT:
						return defaultSqlParameterSource.hasValue(getSourceParameterName(scope, paramName));

					case IN_PROPERTY:
						if (inputSqlParameterSource != null)
							return inputSqlParameterSource.hasValue(getSourceParameterName(scope, paramName));
						break;

					case IN_WHOLE:
						return true;
				}
				
				return false;
			}

			public Object getValue(String paramName) throws IllegalArgumentException {
				final Scope scope = getScope(paramName);

				switch (scope) {
					case DEFAULT:
						return defaultSqlParameterSource.getValue(getSourceParameterName(scope, paramName));
						
					case IN_PROPERTY:
						if (inputSqlParameterSource != null)
							return inputSqlParameterSource.getValue(getSourceParameterName(scope, paramName));
						break;

					case IN_WHOLE:
						return input;
				}
				
				throw new IllegalArgumentException("No value for parameter " + paramName);
			}

			public int getSqlType(String paramName) {
				final Scope scope = getScope(paramName);

				switch (scope) {
					case DEFAULT:
						return defaultSqlParameterSource.getSqlType(getSourceParameterName(scope, paramName));

					case IN_PROPERTY:
						if (inputSqlParameterSource != null)
							return inputSqlParameterSource.getSqlType(getSourceParameterName(scope, paramName));
						break;

					case IN_WHOLE:
						return StatementCreatorUtils.javaTypeToSqlParameterType(m_inputClass);
				}
				
				return TYPE_UNKNOWN;
			}

			public String getTypeName(String paramName) {
				final Scope scope = getScope(paramName);

				switch (scope) {
					case DEFAULT:
						return defaultSqlParameterSource.getTypeName(getSourceParameterName(scope, paramName));

					case IN_PROPERTY:
						if (inputSqlParameterSource != null)
							return inputSqlParameterSource.getTypeName(getSourceParameterName(scope, paramName));
						break;
				}
				
				return null;
			}
		};
	}
}
