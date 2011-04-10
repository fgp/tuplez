package org.phlo.tuplez;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Invocation handler for dynamic proxies ({@link java.lang.reflect.Proxy})
 * that simply returns pre-computed results for all zero-argument
 * functions, provides deep-compare semantics for equals() and
 * hashCode() and lets toString() returns a human-readable string.
 * <p>
 * The canned return values are provided at construction time as
 * a Map<String,Object>, mapping the method's name to its return
 * value.
 * <p>
 * The types of the objects contained in the Map are <b>not</b>
 * verified. If the type of one of the canned return values does
 * not match the interface that proxy was created for, expect a
 * {@link ClassCastException} to be thrown at run-time if the
 * corresponding method is called.
 * 
 * <ul>
 * <li> hashCode() and equals() are forwarded to the respective members
 * of the provided Map instance.
 * 
 * <li> toString() converts the map to a string of the form
 * <pre>{&lt;key&gt;(): &lt;value&gt; ...}</pre>
 * 
 * </ul>
 * @author fgp
 *
 */
final class OutputProxyInvocationHandler implements InvocationHandler {
	/**
	 * Handler for a specific method, whose result is possible
	 * non-immutable (i.e. the method's results depends on the arguments
	 * and/or the current program state).
	 */
	private abstract static class MethodHandler {
		abstract Object handle(Object obj, Object[] args);
	}

	/**
	 * Handle for a specific method whose result is immutable (i.e.
	 * the methods returns the same result on every invocation).
	 *
	 */
	private static class MethodHandlerImmutable extends MethodHandler {
		private final Object m_result;
		
		public MethodHandlerImmutable(Object result) {
			m_result = result;
		}
		
		Object handle(Object obj, Object[] args) {
			assert args == null;
			return m_result;
		}
	}

	/**
	 * Number of additional "support methods" we respond to.
	 * Current these are equals(), toString(), hashCode().
	 */
	private static final int s_supportMethodsCount = 3;
	
	/**
	 * Getter results keyed by the getter's name.
	 */
	private final Map<String, Object> m_getterNameResults;
	
	/**
	 * Order in which the getters results are listed in
	 * toString()
	 */
	private final Collection<String> m_getterToStringOrder;
	
	/**
	 * For efficiency, we generate a MethodHandler instance for
	 * every every Method on the first invocation of that method.
	 * These handlers are cached here, keyed by the Method object.
	 */
	private final ConcurrentMap<Method, MethodHandler> m_methodHandlers;
	
	/**
	 * Creates a new {@link OutputProxyInvocationHandler} with a customized
	 * field ordering for toString().
	 * 
	 * @param getterNameResults Mapping between getter names and results
	 * @param getterToStringOrder Getter order for toString()
	 */
	public OutputProxyInvocationHandler(final Map<String, Object> getterNameResults, final Collection<String> getterToStringOrder) {
		m_getterNameResults = getterNameResults;
		m_getterToStringOrder = getterToStringOrder;
		m_methodHandlers = new java.util.concurrent.ConcurrentHashMap<Method, OutputProxyInvocationHandler.MethodHandler>(
			m_getterNameResults.size() + s_supportMethodsCount
		);
	}

	/**
	 * Creates a new {@link OutputProxyInvocationHandler} with the default
	 * field ordering for toString().

	 * @param getterNameResults Mapping between getter names and results
	 */
	public OutputProxyInvocationHandler(final Map<String, Object> getterNameResults) {
		m_getterNameResults = getterNameResults;
		m_getterToStringOrder = m_getterNameResults.keySet();
		m_methodHandlers = new java.util.concurrent.ConcurrentHashMap<Method, OutputProxyInvocationHandler.MethodHandler>(
			m_getterNameResults.size() + s_supportMethodsCount
		);
	}

	public Object invoke(Object obj, Method method, Object[] args) 	throws Throwable {
		/* Get the method handler for the invoked method if there already is one */
		MethodHandler handler = m_methodHandlers.get(method);
		
		/* Create the method handler on-demand and cache it if we didn't find one
		 * above. 
		 */
		if (handler == null) {
			handler = createHandler(method);
			m_methodHandlers.put(method, handler);
		}
		
		/* Now invoke the handler */
		return handler.handle(obj, args);
	}
	
	/**
	 * Created a new MethodHandler for the given method
	 * 
	 * @param method the method to create a handler instance for
	 * @return handler instance for given method
	 */
	private MethodHandler createHandler(Method method) {
		final String methodName = method.getName();
		final int methodArgCount = method.getParameterTypes().length;
		
		if (m_getterNameResults.containsKey(methodName) && (methodArgCount == 0)) {
			/* #<getter>() */
			
			return new MethodHandlerImmutable(m_getterNameResults.get(methodName));
		}
		else if (methodName.equals("hashCode") && (methodArgCount == 0)) {
			/* #hashCode() */

			return new MethodHandlerImmutable(m_getterNameResults.hashCode());
		}
		else if (methodName.equals("equals") && (methodArgCount == 1)) {
			/* #equals(Object other) */
			
			return new MethodHandler() {
				@Override
				Boolean handle(Object obj, Object[] args) {
					assert (args != null) && (args.length == 1);
					assert m_getterNameResults == ((OutputProxyInvocationHandler)Proxy.getInvocationHandler(obj)).m_getterNameResults;

					Object other = args[0];
					if (obj.getClass() != other.getClass()) {
						/* Other is a different kind of proxy or no proxy at all */
						return false;
					}
					else {
						/* Other is the same kind of proxy. Compare method results */
						OutputProxyInvocationHandler otherHandler = (OutputProxyInvocationHandler)Proxy.getInvocationHandler(args[0]);
						return m_getterNameResults.equals(otherHandler.m_getterNameResults);
					}
				}
				
			};
		}
		else if (methodName.equals("toString") && (methodArgCount == 0)) {
			/* #toString() */

			/* Build result string */
			StringBuilder b;
			{
				b = new StringBuilder();
				b.append("{");
				
				boolean firstEntry = true;
				for(String getter: m_getterToStringOrder) {
					Object result = m_getterNameResults.get(getter);
					
					/* Add separator, except for first entry */
					if (!firstEntry)
						b.append("; ");
					firstEntry = false;
					
					b.append(getter);
					b.append("(): ");
					b.append((result == null) ? "null" : result);
				}
				
				b.append("}");
			}
			
			return new MethodHandlerImmutable(b.toString());
		}
		else {
			return new MethodHandlerImmutable(null);
		}
	}
}
