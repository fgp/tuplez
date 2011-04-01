package org.phlo.tuplez.operation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specified the underlying statement of {@link Operation}s.
 * <p>
 * Every implementation of {@link Operation} must either a
 * {@link Statement} annotation, or implement {@link StatementIsComputed}
 * and provide a {@link StatementIsComputed#getStatement(Object)}
 * member.
 * <p>
 * The mapping between the {@link Operation}'s {@code OutputType}
 * and the statement's result columns can be adjusted by adding
 * a {@link OutputMapping} annotation listing one or more instances
 * of {@link OutputColumn}.
 * 
 * @see OutputMapping
 * @see OutputColumn
 * @see Operation
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Statement {
	String value();
}
