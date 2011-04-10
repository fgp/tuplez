package org.phlo.tuplez;

import javax.sql.DataSource;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.Assert;

import org.phlo.tuplez.operation.Operation;

/**
 * Executes concrete {@link Operation}s.
 * <p>
 * Executor instances are created from {@link DataSource}s,
 * and contain instance methods which execute concrete
 * {@link Operation} implementations.
 * <p>
 * Executor instances are typically created by Spring.
 * This is e.g. done by putting the following into your
 * application context configuration XML (assuming that
 * there is a bean named "DataSource" which defines
 * your database connection).
 *<blockquote><pre>{@literal
 *<bean id="DatabaseExecutor"
 *      class="org.phlo.tuplez.Executor"
 *>
 *    <property name="DataSource" ref="DataSource"/>
 *</bean>
 *}</pre></blockquote>
 *<p>
 * Executor instances also provide the parameters
 * for <b>default.*</b> statement input parameters.
 * These parameters are fetched from the default
 * input object set with {@link #setDefaultInput(Object)}.
 * This property is also typically set by Spring wit
 * a {@literal <property>} tag.
 *<blockquote><pre>{@literal
 *<bean id="DatabaseExecutor"
 *      class="org.phlo.tuplez.Executor"
 *>
 *    <property name="DataSource" ref="DataSource"/>
 *    <property name="DefaultInput">
 *        <bean class="org.example.MyDefaultInputBean"/>
 *    </property>
 *</bean>
 *}</pre></blockquote>
 *
 * @see Operation
 */
public final class Executor implements InitializingBean
{
	/* The named-parameter JDBC template used to execute operations */
	private NamedParameterJdbcTemplate m_npJdbcTemplate;

	/* The parameter source for default.* input parameters */
	private Object m_defaultInput;
	
	/**
	 * Allows construction of {@link Executor} instances
	 * as Java Beans.
	 */
	public Executor() {
		m_npJdbcTemplate = null;
	}
	
	/**
	 * Creates an {@link Executor} instance for the specified
	 * {@link DataSource}.
	 * 
	 * @param dataSource data source to use
	 */
	public Executor(final DataSource dataSource) {
		Assert.notNull(dataSource, "DataSoure must not be null");
		setDataSource(dataSource);
	}
	
	/**
	 * Sets the executor's {@link DataSource}. Usually not
	 * called directly, but instead called by Spring when
	 * your executor bean definition contains
	 *<blockquote><pre>{@literal
	 *    <property name="DataSource" ref="NameOfYourDataSource"/>
	 *}</pre></blockquote>
	 *
	 * @param dataSource data source to use
	 */
	public void setDataSource(final DataSource dataSource) {
		Assert.notNull(dataSource, "DataSoure must not be null");
		m_npJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
	
	/**
	 * Set the executor's default input parameter bean.
	 * <p>
	 * This object is queried when database operations
	 * access default.* parameters.
	 * 
	 * @param defaultInput default input parameter bean
	 */
	public void setDefaultInput(final Object defaultInput) {
		m_defaultInput = defaultInput;
	}
	
	/**
	 * Returns the executor's default input parameter bean.
	 * 
	 * @see #setDefaultInput(Object)
	 * 
	 * @return default input parameter bean
	 */
	public Object getDefaultInput() {
		return m_defaultInput;
	}
	
	/**
	 * Returns the underlying named-parameter JDBC template instances.
	 * 
	 * Can be used to execute queries which are not
	 * represented by concrete implementations of
	 * {@link Operation}
	 * 
	 * @return the underlying named-parameter JDBC template instance
	 */
	public NamedParameterJdbcTemplate getNpJdbcTemplate() {
		return m_npJdbcTemplate;
	}
	
	/**
	 * Returns an instance of the operation defined by
	 * opClass
	 * 
	 * @param <OpType> the operation's type
	 * @param opClass the operation's defining class/interface
	 * @return an instance that is-a opClass
	 */
	public <OpType extends Operation<?,?>> OpType with(final Class<OpType> opClass) {
		return OperationFactory.getFactory(opClass).getInstance(
			m_npJdbcTemplate,
			m_defaultInput
		);
	}
	
	/* org.springframework.beans.factory.InitializingBean */
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(m_npJdbcTemplate, "Property DataSource is required");
	}	
}

