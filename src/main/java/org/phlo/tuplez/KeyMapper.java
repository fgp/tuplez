package org.phlo.tuplez;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.ConcurrentMap;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.support.KeyHolder;

import com.googlecode.gentyref.GenericTypeReflector;

import org.phlo.tuplez.operation.*;

public class KeyMapper<KeyType extends Number> {
	/**
	 * Statement mapper instance cache
	 */
	private static final ConcurrentMap<Class<? extends Operation<?, Void>>, KeyMapper<? extends Number>> s_keyMappers = new java.util.concurrent.ConcurrentHashMap<Class<? extends Operation<?, Void>>, KeyMapper<? extends Number>>();

	/**
	 * Factory methods for StatementMapper instances
	 * 
	 * @param <KeyType> KeyType of the {@link Operation} which {@link GeneratesKey}
	 * @param opClass The concrete {@link Operation}
	 * @return An StatementMapper instance
	 */
	public static <
		KeyType extends Number,
		OpClassType extends Operation<?, Void> & GeneratesKey<KeyType>
	>
	KeyMapper<KeyType>
	getInstance(final Class<OpClassType> opClass)
	{
		@SuppressWarnings("unchecked")
		KeyMapper<KeyType> keyMapper = (KeyMapper<KeyType>)s_keyMappers.get(opClass);
		if (keyMapper == null) {
			keyMapper = new KeyMapper<KeyType>(opClass);
			s_keyMappers.put(opClass, keyMapper);
		}
		
		return keyMapper;
	}
	
	private static interface KeyExtractor<KeyType extends Number> {
		KeyType extractKey(KeyHolder keyHolder);
	}
	
	private final KeyColumn m_generatedKeyAnnotation;
	private final KeyExtractor<? extends Number> m_keyExtractor;
	
	public <
		OpClassType extends Operation<?, Void> & GeneratesKey<KeyType>
	>
	KeyMapper(final Class<OpClassType> opClass)
	{
		/* Get generated key columns */
		m_generatedKeyAnnotation = opClass.getAnnotation(KeyColumn.class);

		/* Get declared key type */
		@SuppressWarnings("unchecked")
		Class<KeyType> keyType = (Class<KeyType>)GenericTypeReflector.getTypeParameter(
			opClass,
			GeneratesKey.class.getTypeParameters()[0]
		);
		
		/* Create extractor for the declared key type */
		
		if (Short.class.isAssignableFrom(keyType)) {
			m_keyExtractor = new KeyExtractor<Short>() {
				@Override
				public Short extractKey(KeyHolder keyHolder) {
					return keyHolder.getKey().shortValue();
				}
			};
		}
		else if (Integer.class.isAssignableFrom(keyType)) {
			m_keyExtractor = new KeyExtractor<Integer>() {
				@Override
				public Integer extractKey(KeyHolder keyHolder) {
					return keyHolder.getKey().intValue();
				}
			};
		}
		else if (Long.class.isAssignableFrom(keyType)) {
			m_keyExtractor = new KeyExtractor<Long>() {
				@Override
				public Long extractKey(KeyHolder keyHolder) {
					return keyHolder.getKey().longValue();
				}
			};
		}
		else if (BigInteger.class.isAssignableFrom(keyType)) {
			m_keyExtractor = new KeyExtractor<BigInteger>() {
				@Override
				public BigInteger extractKey(KeyHolder keyHolder) {
					Number key = keyHolder.getKey();
					if (key instanceof BigInteger)
						return (BigInteger)key;
					else
						throw new InvalidDataAccessApiUsageException(
							"Generated key is declared to be a BigInteger " +
							"but it's actually an " + key.getClass().getName()
						);
				}
			};
		}
		else if (BigDecimal.class.isAssignableFrom(keyType)) {
			m_keyExtractor = new KeyExtractor<BigDecimal>() {
				@Override
				public BigDecimal extractKey(KeyHolder keyHolder) {
					Number key = keyHolder.getKey();
					if (key instanceof BigDecimal)
						return (BigDecimal)key;
					else
						throw new InvalidDataAccessApiUsageException(
							"Generated key is declared to be a BigDecimal " +
							"but it's actually an " + key.getClass().getName()
						);
				}
			};
		}
		else
			throw new InvalidDataAccessApiUsageException(
				"Generated key type " + keyType.getName() + " is not supported"
			);
	}
	
	public String[] getGeneratedKeyColumns() {
		if (m_generatedKeyAnnotation == null)
			return null;
		else
			return new String[] { m_generatedKeyAnnotation.value() };
	}
	
	@SuppressWarnings("unchecked")
	public KeyType mapKey(KeyHolder keyHolder) {
		return (KeyType)m_keyExtractor.extractKey(keyHolder);
	}
}
