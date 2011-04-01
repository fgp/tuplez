package org.phlo.tuplez.operation;

import java.lang.annotation.*;

/**
 * Overrides the OutputType's getter a given column is mapped to
 * and the JDBC {@link java.sql.ResultSet} accessor used to
 * fetch that column's values.
 * 
 * Used together with {@link OutputMapping}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface OutputColumn {
	String column();
	String getter() default "";
	String jdbcAccessor() default "";
}
