package org.phlo.tuplez.operation;

import java.lang.annotation.*;

/**
 * Contains a list of {@link OutputColumn}s, each mapping
 * a certain result row to an OutputType getter and a
 * {@link java.sql.ResultSet} accessor.
 * 
 * Only has an effect when applied to concrete implementations
 * of {@link Operation}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface OutputMapping {
	OutputColumn[] value();
}
