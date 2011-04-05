package org.phlo.tuplez;

import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentMap;

import org.phlo.tuplez.operation.*;

/**
 * Provides support for mapping database operations
 * (implementation of {@link Operation})
 * to statement strings
 */
public final class StatementMapper<InputType> {
	/**
	 * Statement mapper instance cache
	 */
	private static final ConcurrentMap<Class<? extends Operation<?, ?>>, StatementMapper<?>> s_statementMappers = new java.util.concurrent.ConcurrentHashMap<Class<? extends Operation<?, ?>>, StatementMapper<?>>();

	/**
	 * Factory methods for StatementMapper instances
	 * 
	 * @param <InputType> InputType of the {@link Operation}
	 * @param opClass The concrete {@link Operation}
	 * @return An StatementMapper instance
	 */
	public static <InputType>
	StatementMapper<InputType>
	getInstance(final Class<? extends Operation<InputType, ?>> opClass)
	{
		@SuppressWarnings("unchecked")
		StatementMapper<InputType> statementMapper = (StatementMapper<InputType>)s_statementMappers.get(opClass);
		if (statementMapper == null) {
			statementMapper = new StatementMapper<InputType>(opClass);
			s_statementMappers.put(opClass, statementMapper);
		}
		
		return statementMapper;
	}
	
	private final StatementIsComputed<? super InputType> m_statementIsComputedInstance;
	private final Statement m_statementAnnotation;
	
	/**
	 * Constructs an StatementMapper instance for a concrete {@link Operation}
	 * 
	 * @param opClass the class object of the concrete {@link Operation}
	 */
	private StatementMapper(final Class<? extends Operation<InputType, ?>> opClass) {
		/* If the operation implementation implement DatabaseOperationDynamicStatement,
		 * we use that interface's getStatement() member. Otherwise, we look for
		 * @Statement annotations. If both fail, we complain. If both succeed, we
		 * complain too
		 */
		
		if (StatementIsComputed.class.isAssignableFrom(opClass)) {
			/* Statement is generated dynamically. We cast opClass to
			 * the correct type, i.e. we tell the type checker that
			 * it implements StatementIsComputed.
			 */
			@SuppressWarnings("unchecked")
			final Class<? extends StatementIsComputed<InputType>> opStmtIsComputedClass =
				(Class<? extends StatementIsComputed<InputType>>) opClass;
			
			/* Dynamic statement operation. Annotation must not be present. */
			if (opClass.getAnnotation(Statement.class) != null)
				throw new InvalidOperationDefinitionException(
					"class both implements " + StatementIsComputed.class.getSimpleName() + " " +
					"and carries a @" + Statement.class.getSimpleName() + " annotation",
					opClass
				);
			
			/* Sanity checks. These are in principle redundant, but greatly improve the quality
			 * of the error messages
			 */

			/* Is the class static? */
			Class<?> opEnclosingClass = opClass;
			do {
				if ((opClass.getModifiers() & Modifier.STATIC) == 0)
					throw new InvalidOperationDefinitionException("class is a non-static inner class", opClass);
				opEnclosingClass = opEnclosingClass.getEnclosingClass();
			} while (opEnclosingClass != null);
			
			/* Is the class default constructible? */
			try {
				opClass.getConstructor();
			}
			catch (SecurityException e1) {
				throw new InvalidOperationDefinitionException("default constructor is inaccessible", opClass, e1);
			}
			catch (NoSuchMethodException e1) {
				throw new InvalidOperationDefinitionException("class has no default constructor", opClass, e1);
			}
			
			/* Create instance */
			try {
				m_statementIsComputedInstance = opStmtIsComputedClass.newInstance();
			}
			catch (InstantiationException e) {
				throw new InvalidOperationDefinitionException("class could not be instantiated", opClass, e);
			}
			catch (IllegalAccessException e) {
				throw new InvalidOperationDefinitionException("class could not be instantiated", opClass, e);
			}
			
			m_statementAnnotation = null;
		}
		else /* !StatementIsComputed.class.isAssignableFrom(opClass) */
		{
			/* Statement must be provided via a Statement annotation */
			m_statementAnnotation = opClass.getAnnotation(Statement.class);
			m_statementIsComputedInstance = null;
			
			/* Complain if no statement was provided */
			if (m_statementAnnotation == null) {
				throw new InvalidOperationDefinitionException(
					"class neither implements " + StatementIsComputed.class.getSimpleName() + " " +
					"nor does it carry @" + Statement.class.getSimpleName() + " annotation",
					opClass
				);
			}
		}
	}
	
	public String getStatement(final InputType input) {
		if (m_statementIsComputedInstance != null) {
			assert m_statementAnnotation == null;
			return m_statementIsComputedInstance.getStatement(input);
		}
		else {
			return m_statementAnnotation.value();
		}
	}
}
