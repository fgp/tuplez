package org.phlo.tuplez;

import org.phlo.tuplez.operation.Operation;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

@SuppressWarnings("serial")
public class InvalidOperationExecutionException extends InvalidDataAccessResourceUsageException {
	private final Class<? extends Operation<?,?>> m_opClass;
	
	public InvalidOperationExecutionException(final String msg, final Class<? extends Operation<?,?>> opClass) {
		super("Invalid execution of operation " + opClass.getName() + ", " + msg);
		m_opClass = opClass;
	}

	public InvalidOperationExecutionException(final String msg, final Class<? extends Operation<?,?>> opClass, Throwable cause) {
		super("Invalid execution of operation " + opClass.getName() + ", " + msg, cause);
		m_opClass = opClass;
	}

	public Class<? extends Operation<?,?>> getOperation() {
		return m_opClass;
	}

}
