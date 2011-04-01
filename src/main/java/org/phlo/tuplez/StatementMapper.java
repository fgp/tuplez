package org.phlo.tuplez;

import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentMap;

import org.springframework.dao.InvalidDataAccessApiUsageException;

import com.googlecode.gentyref.GenericTypeReflector;

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
		/* Get annotated statement (if any) */
		m_statementAnnotation = opClass.getAnnotation(Statement.class);
		
		/* Check if operation requested dynamic statement */
		if (StatementIsComputed.class.isAssignableFrom(opClass)) {
			/* Do the type parameter to DatabaseOperation and StatementIsComputed match?
			 * Note that we don't require the types to be equal, we just require
			 * the operation's input type to be assignable to whatever was specified
			 * as InputType of StatementIsComputed.
			 */
			Class<? >opInputType = (Class<?>)GenericTypeReflector.getTypeParameter(
				opClass,
				OperationInput.class.getTypeParameters()[0]
			);
			Class<? >statementIsComputedInputType = (Class<?>)GenericTypeReflector.getTypeParameter(
				opClass,
				StatementIsComputed.class.getTypeParameters()[0]
			);
			if (!statementIsComputedInputType.isAssignableFrom(opInputType))
				throw new InvalidDataAccessApiUsageException(
					"Database operation class " + opClass.getName() + " " +
					"specifies incompatible types for the InputTypes of " +
					"StatementIsComputed and DatabaseOperation"
				);
			@SuppressWarnings("unchecked")
			final Class<? extends StatementIsComputed<? super InputType>> opClassStatementIsComputed =
				(Class<? extends StatementIsComputed<? super InputType>>)opClass;
			
			/* Dynamic statement operation. Annotation must not be present. */
			if (m_statementAnnotation != null)
				throw new InvalidDataAccessApiUsageException(
					"Ambiguity while determining statement for " + opClass.getName() + ". " +
					"Class both implements " + StatementIsComputed.class.getSimpleName() + " " +
					"and carries a @" + Statement.class.getSimpleName() + " annotation."
				);
			
			/* Sanity checks. These are in principle redundant, but greatly improve the quality
			 * of the error messages
			 */

			/* Is the class static? */
			Class<?> opEnclosingClass = opClass;
			do {
				if ((opClass.getModifiers() & Modifier.STATIC) == 0)
					throw new InvalidDataAccessApiUsageException("Database operation class " + opClass.getName() + " is a non-static inner class");
				opEnclosingClass = opEnclosingClass.getEnclosingClass();
			} while (opEnclosingClass != null);
			
			/* Is the class default constructible? */
			try {
				opClass.getConstructor();
			}
			catch (SecurityException e1) {
				throw new InvalidDataAccessApiUsageException("Database operation class " + opClass.getName() + "'s default constructor is inaccessible");
			}
			catch (NoSuchMethodException e1) {
				throw new InvalidDataAccessApiUsageException("Database operation class " + opClass.getName() + " has no default constructor");
			}
			
			/* Create instance */
			try {
				m_statementIsComputedInstance = opClassStatementIsComputed.newInstance();
			}
			catch (InstantiationException e) {
				throw new InvalidDataAccessApiUsageException(
					"Failed to instanciate dynamic statement database operation " +
					opClass.getName(),
					e
				);
			}
			catch (IllegalAccessException e) {
				throw new InvalidDataAccessApiUsageException(
					"Insufficient permissions to instanciate dynamic statement database operation " +
					opClass.getName(),
					e
				);
			}
		}
		else /* !StatementIsComputed.class.isAssignableFrom(opClass) */
		{
			/* Static statement operation. Annotation must be present */
			if (m_statementAnnotation == null) {
				throw new InvalidDataAccessApiUsageException(
					"Failed to determine statement for " + opClass.getName() + ". " +
					"Class neither implements " + StatementIsComputed.class.getSimpleName() + " " +
					"nor does it carry @" + Statement.class.getSimpleName() + " annotation."
				);
			}
			
			m_statementIsComputedInstance = null;
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
