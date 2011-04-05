package org.phlo.tuplez;

import org.springframework.dao.InvalidDataAccessApiUsageException;

import org.phlo.tuplez.operation.*;

@SuppressWarnings("serial")
public final class InvalidOperationDefinitionException extends InvalidDataAccessApiUsageException {
	private final Class<? extends Operation<?,?>> m_opClass;
	
	public InvalidOperationDefinitionException(final String msg, final Class<? extends Operation<?,?>> opClass) {
		super("Invalid definition of operation " + opClass.getName() + ", " + msg);
		m_opClass = opClass;
	}

	public InvalidOperationDefinitionException(final String msg, final Class<? extends Operation<?,?>> opClass, Throwable cause) {
		super("Invalid definition of operation " + opClass.getName() + ", " + msg, cause);
		m_opClass = opClass;
	}

	public Class<? extends Operation<?,?>> getOperation() {
		return m_opClass;
	}
}
