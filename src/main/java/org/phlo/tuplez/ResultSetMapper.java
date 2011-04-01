package org.phlo.tuplez;

import java.lang.reflect.*;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.UncategorizedSQLException;

import com.googlecode.gentyref.GenericTypeReflector;

import org.phlo.tuplez.operation.*;

/**
 * Provides support for mapping the rows resulting from
 * an execution of an {@link Operation}'s SQL
 * statement to objects conforming to the operation's
 * declared output type "OutputType".
 * 
 * ResultSetMapper instances are typically obtained
 * by calling {@link ResultSetMapper#getInstance}, which
 * will create one ResultMapper instance per combination
 * of statement call and ResultSet class.
 * 
 * @param <OutputType> type repesenting result rows
 */
public final class ResultSetMapper<OutputType> {
	/**
	 * ResultSetMapper instances created by {@link getInstance}
	 */
	static private final ConcurrentMap<ID<?>, ResultSetMapper<?>> s_resultSetMappers = new java.util.concurrent.ConcurrentHashMap<ID<?>, ResultSetMapper<?>>();
	
	/**
	 * Mapping between the {@link Class} instances representing
	 * primitive types (like int) and the corresponding {@link Class}
	 * instances representing their boxed (object) types (like Integer)
	 */
	static private Map<Class<?>, Class<?>> s_classPrimitiveToBoxed = new java.util.HashMap<Class<?>, Class<?>>();
	static {
		s_classPrimitiveToBoxed.put(Boolean.TYPE, Boolean.class);
		s_classPrimitiveToBoxed.put(Character.TYPE, Character.class);
		s_classPrimitiveToBoxed.put(Byte.TYPE, Byte.class);
		s_classPrimitiveToBoxed.put(Short.TYPE, Short.class);
		s_classPrimitiveToBoxed.put(Integer.TYPE, Integer.class);
		s_classPrimitiveToBoxed.put(Long.TYPE, Long.class);
		s_classPrimitiveToBoxed.put(Float.TYPE, Float.class);
		s_classPrimitiveToBoxed.put(Double.TYPE, Double.class);
	}
	
	/**
	 * Mapping between the {@link Class} instances representing
	 * boxed primitive types (like Integer) and their corresponding
	 * unboxed types (like int).
	 */
	static private Map<Class<?>, Class<?>> s_classBoxedToPrimitive = new java.util.HashMap<Class<?>, Class<?>>();
	static {
		for(Map.Entry<Class<?>, Class<?>> m: s_classPrimitiveToBoxed.entrySet())
			s_classBoxedToPrimitive.put(m.getValue(), m.getKey());
	}
	
	/**
	 * List of JDBC accessor methods that are ignored when
	 * autodetecting the JDBC accessor based on the output
	 * type's accessor's return type.
	 */
	static private Set<String> s_jdbcAccessorBlackList = new java.util.HashSet<String>();
	static {
		s_jdbcAccessorBlackList.add("getNString");
		s_jdbcAccessorBlackList.add("getLength");
	}
	
	/**
	 * Encapsulates the identifying parameters of a ResultSetMapper instance.
	 */
	static private final class ID<OutputType> {
		final public Class<? extends Operation<?, OutputType>> opClass;
		final private Class<? extends ResultSet> rsClass;
		
		ID(
			final Class<? extends Operation<?, OutputType>> _opClass,
			final Class<? extends ResultSet> _rsClass
		) {
			opClass = _opClass;
			rsClass = _rsClass;
		}
			
		@Override
		public int hashCode() {
			return
			opClass.hashCode() ^
			rsClass.hashCode();
		}

		@Override
		public boolean equals(Object other) {
			if (this == other)
				return true;
			if (!(other instanceof ID))
				return false;
			
			ID<?> otherID = (ID<?>)other;
			if (opClass != otherID.opClass)
				return false;
			if (rsClass != otherID.rsClass)
				return false;
			return true;
		}
	}
		
	/**
	 * Factory method for ResultSetMapper instances.
	 * 
	 * Returns a cached ResultSetMapper if one exists with the same
	 * opClass and resultSet.class. Otherwise a new ResultSetMapper
	 * is created and put into the cache.
	 * 
	 * Note that ResultSetMapper instances depend on the ResultSet's
	 * meta data! For performance reasons, the meta data is only
	 * fetched and processed during initial construction of a 
	 * ResultSetMapper. A single operation must thus always produce
	 * ResultSets with the same meta data, otherwise row extraction
	 * will fail.
	 * 
	 * @param opClass the operation's class
	 * @param resultSet the ResultSet produced by the operation's SQL statement
	 * @param <OutputType> the operation's declared output type
	 * @return a ResultSetMapper instance
	 * @throws SQLException
	 */
	public static <OutputType> ResultSetMapper<OutputType> getInstance(
		Class<? extends Operation<?, OutputType>> opClass,
		ResultSet resultSet
	) throws SQLException
	{
		/* ResultSet class */
		Class<? extends ResultSet> resultSetClass = resultSet.getClass();
		
		/* Statement class and ResultSet class uniquely identify
		 * a specific ResultSetMapper
		 */
		ID<OutputType> rsmID = new ID<OutputType>(opClass, resultSetClass);
		
		/* Return cached ResultSetMapper or create new one if
		 * none already exists
		 */
		@SuppressWarnings("unchecked")
		ResultSetMapper<OutputType> resultSetMapper = (ResultSetMapper<OutputType>)s_resultSetMappers.get(rsmID);
		if (resultSetMapper == null) {
			/* Get ResultSet meta data */
			ResultSetMetaData resultSetMetaData;
			try {
				resultSetMetaData = resultSet.getMetaData();
			}
			catch (SQLException e) {
				throw new UncategorizedSQLException("Failed to retrieve result set meta data", null, e);
			}
			
			/* Create new ResultSetMapper */
			resultSetMapper = new ResultSetMapper<OutputType>(
				opClass,
				resultSetClass,
				resultSetMetaData
			);
		
			/* Add new ResultSetMapper to the Cache. */
			s_resultSetMappers.put(rsmID, resultSetMapper);
		}
		
		return resultSetMapper;
	}
	
	/**
	 * Per-Column meta data computed by the ResultSetMapper
	 */
	static private final class ColumnMetaData {
		public String columnName;
		public Class<?> columnClass;
		public String getterName;
		public Method getterMethod;
		public String jdbcAccessorName;
		public Method jdbcAccessorMethod;
	}

	/* Column meta data. Computed during construction */
	final private List<ColumnMetaData> m_columns = new java.util.ArrayList<ColumnMetaData>();
	
	/* Used to control the order of the fields in MapProxyInvocationHandler's toString()
	 * method
	 */
	final private Collection<String> m_getterOrder = new java.util.ArrayList<String>();
	
	/* Output object meta data */
	final private Class<?> m_outputClass;
	
	/* Whether to use the first column's result as output */
	final boolean m_firstColumnIsOutput;
	
	private ResultSetMapper(
		final Class<? extends Operation<?, ?>> opClass,
		final Class<? extends ResultSet> rsClass,
		final ResultSetMetaData rsMeta
	) throws SQLException
	{
		/* Get OutputType of statement class */
		m_outputClass = (Class<?>)GenericTypeReflector.getTypeParameter(
			opClass,
			OperationOutput.class.getTypeParameters()[0]
		);
		if (m_outputClass == null)
			throw new InvalidDataAccessApiUsageException("Unable to determine statement's output class");
				
		/* First, fetch column names */
		int colNr = rsMeta.getColumnCount();
		for(int colIdx = 1; colIdx <= colNr; ++colIdx) {
			/* Create meta-data object for the column. For now,
			 * only set the name, the rest will be filled in below
			 */
			ColumnMetaData colMeta = new ColumnMetaData();
			colMeta.columnName = rsMeta.getColumnName(colIdx);
			m_columns.add(colMeta);
		}
		
		/* Then, fill out getter and jdbcAccessor for the fields
		 * which have a ResultMapping annotation
		 */
		{
			OutputMapping outputInfos = opClass.getAnnotation(OutputMapping.class);
			if (outputInfos != null) {
				for(OutputColumn outputInfo: outputInfos.value()) {
					for(ColumnMetaData colMeta: m_columns) {
						/* Join by name */
						if (!colMeta.columnName.equals(outputInfo.column()))
							continue;
						
						/* Annotations don't allow null values, thus
						 * treat empty strings as null
						 */
						
						if (!outputInfo.getter().isEmpty())
							colMeta.getterName = outputInfo.getter();
	
						if (!outputInfo.jdbcAccessor().isEmpty())
							colMeta.jdbcAccessorName = outputInfo.jdbcAccessor();
					}
						
				}
			}
		}
		
		/* Resolve OutputType getter methods for the individual columns.
		 * If the column wasn't explicitly mapped to a certain getter
		 * by an annotation (parsed above), its mapped by name. Once
		 * the getter method is resolved, the column's mapped type can
		 * be inferred from the getter's result type.
		 * 
		 * For single-column result sets where the user specified
		 * no explicit OutputType getter by an annotation, we also
		 * check if there is a ResultSet accessor which returns a value
		 * compatible with the whole OutputType. If there is, we use that
		 * directly.
		 */
		resolve_getters:
		{
			/* Check if we should try the single-column mode */
			if ((m_columns.size() == 1) &&
				(m_columns.get(0).getterName == null)
			) {
				ColumnMetaData colMeta = m_columns.get(0);
				
				/* Single-column result and no explicit getter specification.
				 * See if we can obtain an instance of OutputType from the
				 * JDBC result set directly.
				 */
				assert colMeta.columnClass == null;
				colMeta.columnClass = m_outputClass;
				if (computeJdbcAccessor(colMeta, rsClass)) {
					/* Success */
					m_firstColumnIsOutput = true;
					break resolve_getters;
				}
				else {
					/* Failure */
					colMeta.columnClass = null;
				}
			}
			
			/* Single-column mode didn't work out */
			m_firstColumnIsOutput = false;
			for(ColumnMetaData colMeta: m_columns) {
				if (!computeOutputGetter(colMeta))
					throw new DataRetrievalFailureException("No getter for " + colMeta.columnName + " found in " + m_outputClass.getName());
			}
			
			/* Store getter order */
			for(ColumnMetaData colMeta: m_columns)
				m_getterOrder.add(colMeta.getterName);
		}
		
		/* Find the JDBC ResultSet accessor method to use.*/
		{
			for(ColumnMetaData colMeta: m_columns) {
				if (!computeJdbcAccessor(colMeta, rsClass))
					throw new DataRetrievalFailureException("No JDBC accessor for column " + colMeta.columnName + " with type " + colMeta.columnClass.getName() + " found");
			}
		}
	}
	
	static final Pattern s_patternNonAlphaNumeric = Pattern.compile("[^A-Za-z0-9]*");
	private boolean computeOutputGetter(final ColumnMetaData colMeta) {
		/* Skip columns who are already mapped to an OutputType getter */
		if (colMeta.getterMethod != null)
			assert colMeta.jdbcAccessorName != null;
		
		if (colMeta.getterName != null) {
			/* The getter's name was specified, we just need to lookup the method */
			try {
				colMeta.getterMethod = m_outputClass.getMethod(colMeta.getterName);
			}
			catch (NoSuchMethodException e) {
				throw new InvalidDataAccessApiUsageException("No getter named " + colMeta.getterName + " for column " + colMeta.columnName + " in" + m_outputClass.getName(), e);
			}
		}
		else {
			/* No getter name was specified. We try to auto-detect based on
			 * the getter name. Since method names in java are often
			 * CamelCased, while database field names are often wholly in
			 * lower or upper case and user underscores as separator, we
			 * convert the names to lowercase and remove all non-alphanumeric
			 * characters before comparing.
			 */
	
			String getterNameComparableColumnDerived = 
				"get" +
				s_patternNonAlphaNumeric
					.matcher(colMeta.columnName.toLowerCase(Locale.ENGLISH))
					.replaceAll("");
			
			Set<Method> candidates = new java.util.HashSet<Method>();
			for(Method candidate: m_outputClass.getMethods()) {
				String getterNameComparableCandidateDerived = s_patternNonAlphaNumeric
						.matcher(candidate.getName().toLowerCase(Locale.ENGLISH))
						.replaceAll("");
				if (!getterNameComparableColumnDerived.equals(getterNameComparableCandidateDerived))
					continue;
				
				/* Names match */
				
				candidates.add(candidate);
			}
			
			/* Check auto-detection result. If we found exactly one candidate,
			 * we're good. We complain if the result was ambiguous, but return
			 * false if no getter was found
			 */
			
			switch (candidates.size()) {
				case 0:
					break;
		
				case 1:
					colMeta.getterMethod = candidates.iterator().next();
					break;
		
				default:
					StringBuilder b = (new StringBuilder())
						.append("Ambiguous getter for column ")
						.append(colMeta.columnName)
						.append(" in ")
						.append(m_outputClass.getName())
						.append("The candidates are |");
					for(Method c: candidates) {
						b.append(c.toString());
						b.append("|");
					}
					throw new DataRetrievalFailureException(b.toString());
			}
		}
		
		/* Update meta data and report success or failure */
		if (colMeta.getterMethod != null) {
			colMeta.getterName = colMeta.getterMethod.getName();
			colMeta.columnClass = colMeta.getterMethod.getReturnType();
			return true;
		}
		else {
			return false;
		}
	}
	
	/**
	 * Resolve the JDBC ResultSet accessor method.
	 * <p>
	 * If a jdbAccessorName is already set (usually because
	 * it was explicitly specified by an annotation), the
	 * corresponding method is simply looked up.
	 * <p>
	 * Otherwise, the accessor method is auto-detected by
	 * comparing the ResultSet's get* method's return type
	 * with the column type (which must already haven been
	 * determined)
	 *
	 * @param colMeta column meta data
	 * @param rsClass result set class
	 */
	private boolean computeJdbcAccessor(final ColumnMetaData colMeta, final Class<? extends ResultSet> rsClass) {
		/* Skip columns who already are mapped to a ResultSet accessor method */
		if (colMeta.jdbcAccessorMethod != null) {
			assert colMeta.jdbcAccessorName != null;
			return true;
		}
		
		if (colMeta.jdbcAccessorName != null) {
			/* The accessor's name was specified, we just need to lookup the method */
			try {
				colMeta.jdbcAccessorMethod = rsClass.getMethod(colMeta.jdbcAccessorName, new Class<?>[] {Integer.TYPE});
			}
			catch (NoSuchMethodException e) {
				throw new InvalidDataAccessApiUsageException("JDBC Accessor " + colMeta.jdbcAccessorName + " for column " + colMeta.columnName + " does not exist in " + rsClass.getName(), e);
			}
			return true;
		}

		/* No accessor name was specified. We try auto-detection based
		 * on the column's type. To resolve ambiguities, we distinguish
		 * four matching levels.
		 *   0: Method from ResultSet interface, return type equals column type
		 *   1: Method from ResultSet interface, return type assignable to column type
		 *   2: Method from concrete ResultSet implementation, return type equals column type
		 *   3: Method from concrete ResultSet implementation, return type equals column type
		 * 
		 * We complain if either no candidates are found, or if the first non-empty
		 * category contains multiple accessors
		 */

		List<Set<Method>> candidateLevels = new java.util.ArrayList<Set<Method>>();
		candidateLevels.add(new java.util.HashSet<Method>());
		candidateLevels.add(new java.util.HashSet<Method>());
		candidateLevels.add(new java.util.HashSet<Method>());
		candidateLevels.add(new java.util.HashSet<Method>());
		for(Method candidate: rsClass.getMethods()) {
			String candidateName = candidate.getName();
			/* We're only interested in column value accessors,
			 * not other methods of ResultSet. We thus limit
			 * the search to methods named get* and which take
			 * exactly one parameter of type "int". We also
			 * skip candidates listed in the JDBC accessor
			 * black list
			 */
			
			if (!candidateName.substring(0,3).equals("get") ||
			    s_jdbcAccessorBlackList.contains(candidateName)
			) {
				continue;
			}
			
			Class<?>[] candidateParameters = candidate.getParameterTypes();
			if (!(candidateParameters.length == 1) || !(candidateParameters[0].isAssignableFrom(Integer.TYPE)))
				continue;
			
			/* The accessor's return type must be convertible to the
			 * column's declared type.
			 * 
			 * NOTE: The column's declared class is never a primitive type,
			 * since those cannot represent null values. But the ResultSet
			 * accessors (like getInt()) do return primitive types (
			 * as opposed to their boxed equivalents, e.g. getInt() returns
			 * int, not Integer). We must thus map one to the other to
			 * be able to detect ResultSet accessors for primitive types
			 */
			Class<?> candidateClass = candidate.getReturnType();
			if (candidateClass.isPrimitive()) {
				if (!s_classPrimitiveToBoxed.containsKey(candidateClass))
					throw new DataRetrievalFailureException("Primitive type " + candidateClass.getName() + " is not supported. NOTE: This probably means that ResultSetMapper needs to be tought about this type!");
				candidateClass = s_classPrimitiveToBoxed.get(candidateClass);
			}
			if (!colMeta.columnClass.isAssignableFrom(candidateClass))
				continue;
			
			/* Found a candidate!
			 * 
			 * Now compute it's matching level
			 */
			int level;
			
			/* Is it declared by the ResultSet interface? */
			level = 3;
			try {
				if (ResultSet.class.getMethod(candidateName, Integer.TYPE).getReturnType().equals(candidateClass))
					level = 0;
			}
			catch (NoSuchMethodException e) {
				/* Ignore */
			}
			
			/* Does the accessor's return type match the column type exactly? */
			if (!candidateClass.equals(colMeta.columnClass))
				level += 1;
			
			/* Store */
			candidateLevels.get(level).add(candidate);
		}

		/* Extract accessor from candidates per the rules
		 * outlined above
		 */
		extract_candidate:
		for(Set<Method> candidates: candidateLevels) {
			switch (candidates.size()) {
				case 0:
					continue extract_candidate;

				case 1:
					colMeta.jdbcAccessorMethod = candidates.iterator().next();
					break extract_candidate;

				default:
					StringBuilder b = (new StringBuilder())
						.append("Ambigous JDBC accessors for column ")
						.append(colMeta.columnName)
						.append(" with type ")
						.append(colMeta.columnClass)
						.append("The candidates are |");
					for(Method c: candidates) {
						b.append(c.toString());
						b.append("|");
					}
					throw new DataRetrievalFailureException(b.toString());
			}
		}
		
		/* Update meta data and report success or failure */
		if (colMeta.jdbcAccessorMethod != null) {
			colMeta.jdbcAccessorName = colMeta.jdbcAccessorMethod.getName();
			return true;
		}
		else {
			return false;
		}
	}
	
	public OutputType mapCurrentRow(ResultSet resultSet) throws SQLException {
		final Map<String, Object> values = new java.util.HashMap<String, Object>();
		
		/* Extract field values from ResultSet and place into <value>,
		 * indexed by the output class's getters names
		 */
		int colIdx = 0;
		for(ColumnMetaData colMeta: m_columns) {
			++colIdx;
			
			/* Fetch column value using the ResultSet accessor we
			 * so painstakingly figured out
			 */
			Object colValue;
			try {
				colValue = colMeta.jdbcAccessorMethod.invoke(resultSet, colIdx);		
			}
			catch (IllegalAccessException e) {
				throw new InvalidDataAccessApiUsageException("Insufficient permissions to fetch the value of column " + colMeta.columnName + " using " + colMeta.jdbcAccessorMethod.toString(), e);
			}
			catch (InvocationTargetException e) {
				if (e.getTargetException() instanceof SQLException)
					throw (SQLException)e.getTargetException();
				else
					throw new InvalidDataAccessApiUsageException("Unable to fetch the value of column " + colMeta.columnName + " using " + colMeta.jdbcAccessorMethod.toString(), e);
			}
			
			/* Since the accessor for primitive types return unboxed values,
			 * their return value cannot represent null. We must thus use
			 * wasNull() which returns true if the last value accessed by some
			 * column accessor actually was null.
			 */
			if (resultSet.wasNull())
				colValue = null;
			
			/* In first-column output mode, we're done */
			if (m_firstColumnIsOutput) {
				assert colIdx == 1;
				@SuppressWarnings("unchecked")
				OutputType output = (OutputType)colValue;
				return output;
			}
			
			/* Finally store the value, keyed by the getter's name for the proxy's
			 * convenience
			 */
			values.put(colMeta.getterName, colValue);
		}
		
		/* Obviously not in first-column output mode */
		assert !m_firstColumnIsOutput;
		
		/* Creates a proxy to turn the hash into an instance of the output class */
		@SuppressWarnings("unchecked")
		OutputType output = (OutputType)Proxy.newProxyInstance(
			m_outputClass.getClassLoader(),
			new Class<?>[] {m_outputClass},
			new OutputProxyInvocationHandler(values, m_getterOrder)
		);
		return output;
	}
}
