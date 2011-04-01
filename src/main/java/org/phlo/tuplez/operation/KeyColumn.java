package org.phlo.tuplez.operation;

import java.lang.annotation.*;

/**
 * Specified the column containg the
 * generated key. The column name is
 * case-sensitive!
 * <p>
 * Useful only on concrete {@link Operation}
 * implementations which are marked as
 * {@link GeneratesKey}.
 * <p>
 * If this annotation is present, the
 * operation is executed with
 * {@link java.sql.Statement#executeUpdate(String, String[]) Statement#executeUpdate}(sql, new String[] {keyColumn})}
 * <p>
 * If this annotation isn't present,  the
 * operation is executed with
 * {@link java.sql.Statement#executeUpdate(String, int) Statement#executeUpdate}(sql, Statement.RETURN_GENERATED_KEYS)}
 * <p>
 * Depending on the JDBC driver's features, only
 * one if these methods may work. The postgres
 * JDBC driver, for example, fails in the second
 * case since it treats <b>all</b> the table's
 * columns as generated if no explicit column
 * list is supplied.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KeyColumn {
	String value();
}
