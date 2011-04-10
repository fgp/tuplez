package org.phlo.tuplez;

import java.lang.reflect.*;
import java.util.concurrent.ConcurrentMap;

import net.sf.cglib.proxy.*;

import org.phlo.tuplez.operation.*;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Creates operation instances by weaving
 * operation definitions (i.e. interface
 * or abstract class definitions which
 * implement {@link Operation} and friends)
 * with {@link OperationDefaultImplementation}.
 * <p>
 * For every operation definition, a separate
 * {@link OperationFactory} instance is created.
 *
 * @param <OpType> the operation's defining type (class or interface)
 */
final class OperationFactory<OpType extends Operation<?,?>> {
	/**
	 * Cache of factory instances, one per operation class/interface  
	 */
	private static final ConcurrentMap<Class<? extends Operation<?, ?>>, OperationFactory<?>> s_operationFactories =
		new java.util.concurrent.ConcurrentHashMap<Class<? extends Operation<?, ?>>,OperationFactory<?>>();
	
	/**
	 * Returns a factory instance for a certain operation type.
	 * <p>
	 * Returns the cached instance for that type unless none exists,
	 * in which case a new one is created.
	 * 
	 * @param <OpType> the operation's type
	 * @param opClass the operation's class
	 * @return a suitable factory instance
	 */
	public static <
		OpType extends Operation<?,?>
	> OperationFactory<OpType> getFactory(
		Class<OpType> opClass
	) {
		@SuppressWarnings("unchecked")
		OperationFactory<OpType> opFactory = (OperationFactory<OpType>)s_operationFactories.get(opClass);
		if (opFactory == null) {
			validateOperation(opClass);
			opFactory = new OperationFactory<OpType>(opClass);
			s_operationFactories.put(opClass, opFactory);
		}
		
		return opFactory;
	}
	
	private static void validateOperation(Class<? extends Operation<?,?>> opClass) {
		//XXX: Implement me!
	}
	
	/**
	 * Generates the callback information required
	 * to generate an operation's proxy class with
	 * cglib's {@link Enhancer}.
	 * <p>
	 * In the non-delegate case no methods are
	 * intercepted.
	 * <p>
	 * In the delegate case, abstract methods
	 * are intercepted and redirected to the
	 * delegate. {@link Object#clone} is
	 * handled specially, since we must make
	 * sure to clone the delegate as well as
	 * the actual operation instance.
	 * <p>
	 * Each {@link CallbackIndices} instance
	 * represents one {@link Callback} instace.
	 * {@link #index} is the index into the
	 * {@code Callback[]} array passed to
	 * the {@link Enhancer}.
	 */
	private static enum CallbackIndices {
		INVOKE_SUPER(0),
		INVOKE_DELEGATE(1),
		INVOKE_CLONE(2);

		static final Class<?>[] CallbackTypesWithoutDelegate = new Class<?>[] {
			/* INVOKE_SUPER */
			NoOp.class
		};

		static CallbackFilter CallbackFilterWithoutDelegat = new CallbackFilter() {
			@Override public int accept(Method method) {
				assert !Modifier.isAbstract(method.getModifiers());
				
				return CallbackIndices.INVOKE_SUPER.index;
			}
		};

		static Callback[] getCallbacksWithoutDelegate() {
			/* INVOKE_SUPER */ return new Callback[] { NoOp.INSTANCE };
		}
		
		static final Class<?>[] CallbackTypesWithDelegate = new Class<?>[] {
			/* INVOKE_SUPER */ NoOp.class,
			/* INVOKE_DELEGATE */ LazyLoader.class,
			/* INVOKE_CLONE */ MethodInterceptor.class
		};
		
		static CallbackFilter CallbackFilterWithDelegate = new CallbackFilter() {
			@Override public int accept(Method method) {
				if (!Modifier.isStatic(method.getModifiers()) && method.getName().equals("clone") && (method.getParameterTypes().length == 0))
					return CallbackIndices.INVOKE_CLONE.index;
				else if (Modifier.isAbstract(method.getModifiers()))
					return CallbackIndices.INVOKE_DELEGATE.index;
				else
					return CallbackIndices.INVOKE_SUPER.index;
			}
		};
				
		static Callback[] getCallbacksWithDelegate(final OperationDefaultImplementation delegate) {
			return new Callback[] {
				/* INVOKE_SUPER */
				NoOp.INSTANCE,
				
				/* INVOKE_DELEGATE */
				new LazyLoader() {
					@Override public OperationDefaultImplementation loadObject() throws Exception {
						assert delegate != null;
						return delegate;
					}
				},
				
				/* INVOKE_CLONE */
				new MethodInterceptor() {
					@Override public Operation<?,?> intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
						assert obj != null;
						assert method != null;
						assert !Modifier.isStatic(method.getModifiers());
						assert method.getName().equals("clone");
						assert args != null;
						assert args.length == 0;
						assert proxy != null;

						/* Clone the operation instance */
						Operation<?,?> clone = (Operation<?,?>)proxy.invokeSuper(obj, args);

						/* Clone the associated default implementation and set
						 * the actual implementation reference of the new clone
						 * to the cloned operation instance
						 */
						final OperationDefaultImplementation opDefaultImpl = delegate.clone();
						opDefaultImpl.m_operation = clone;
						
						/* Make the cloned operation instance use the new delegate */
						((Factory)clone).setCallbacks(getCallbacksWithDelegate(opDefaultImpl));

						return clone;
					}
					
				}
			};
		}
		
		/* Index into the {@code Callback[]} array */
		final int index;
		
		private CallbackIndices(int _index) {
			index = _index;
		}
	}
	
	/**
	 * The different kinds of operation
	 * definition we support.
	 */
	private static enum OperationClassFlavour {
		INTERFACE,
		ABSTRACT,
		INSTANTIABLE
	}
	
	/**
	 * The operation's defining type this factory
	 * is fore
	 */
	final Class<OpType> m_opClass;
	
	/**
	 * The kind of operation class we're dealing with.
	 * Decides how the implementation class is constructed.
	 */
	final OperationClassFlavour m_opClassFlavour;
	
	/**
	 * The operation instance prototype. {@link Enhancer}
	 * offer the best performance when instances are
	 * created from a prototype instance by using
	 * the {@link Factory} interface every class
	 * created by {@link Enhancer} implements.
	 * <p>
	 * null iff m_opClassFlavour == INSTANTIABLE
	 */
	final Factory m_factory;
	
	/**
	 * The operation's constructor. Used only
	 * if no class generated is necessary.
	 * <p>
	 * not null iff m_opClassFlavour == INSTANTIABLE
	 */
	final Constructor<OpType> m_constructor;
	
	/**
	 * Creates a prototype instance {@link #m_factory}
	 * for the operation with defining type opClass
	 */
	private OperationFactory(final Class<OpType> opClass) {
		m_opClass = opClass;
		
		Enhancer enhancer = new Enhancer();
        enhancer.setUseFactory(true);
        enhancer.setUseCache(false);
        enhancer.setInterceptDuringConstruction(false);
		
		if (m_opClass.isInterface()) {
			/* The operation's defining type is an interface.
			 * We can simply derive the implementation class
			 * from the default implementation and thus don't
			 * need any method interception at all.
			 */
			m_opClassFlavour = OperationClassFlavour.INTERFACE;
					
			enhancer.setSuperclass(OperationDefaultImplementation.class);
			enhancer.setInterfaces(new Class<?>[] {m_opClass});
			enhancer.setCallbackTypes(CallbackIndices.CallbackTypesWithoutDelegate);
			enhancer.setCallbackFilter(CallbackIndices.CallbackFilterWithoutDelegat);
			enhancer.setCallbacks(CallbackIndices.getCallbacksWithoutDelegate());
			
			/* Create prototype instance using the constructor
			 * (NamedParameterJdbcTemplate, DefaultInput)
			 */
			m_factory = (Factory)enhancer.create(
				new Class<?>[] {Class.class, NamedParameterJdbcTemplate.class, Object.class},
				new Object[] {m_opClass, null, null}
			);
			m_constructor = null;
		}
		else if (Modifier.isAbstract(m_opClass.getModifiers())) {
			/* The operation's defining type is an abstract class.
			 * We create a subclass and delegate invocations of
			 * abstract methods to the default implementation
			 */
			m_opClassFlavour = OperationClassFlavour.ABSTRACT;
						
			enhancer.setSuperclass(m_opClass);
			enhancer.setInterfaces(new Class<?>[] {});
			enhancer.setCallbackTypes(CallbackIndices.CallbackTypesWithDelegate);
			enhancer.setCallbackFilter(CallbackIndices.CallbackFilterWithDelegate);
			enhancer.setCallbacks(CallbackIndices.getCallbacksWithDelegate(
				new OperationDefaultImplementation(m_opClass, null, null))
			);
			
			/* Create prototype instance using the no-args constructor */
			m_factory = (Factory)enhancer.create();
			m_constructor = null;
		}
		else {
			/* The operation's defining class is instantiable.
			 * We don't need to generate any class
			 */
			m_opClassFlavour = OperationClassFlavour.ABSTRACT;
			m_factory = null;
			try {
				/* Lookup constructor
				 * (NamedParameterJdbcTemplate, DefaultInput)
				 */
				m_constructor = m_opClass.getConstructor(
					NamedParameterJdbcTemplate.class,
					Object.class
				);
			}
			catch (NoSuchMethodException e) {
				throw new RuntimeException("constructor not found");
			}
		}
		
	}
	
	public OpType getInstance(
		final NamedParameterJdbcTemplate npJdbcTemplate,
		final Object defaultInput
	) {
		try {
			switch (m_opClassFlavour) {
				case INTERFACE: {
					/* Create new instance using the constructor
					 * (NamedParameterJdbcTemplate, DefaultInput)
					 * of OperationDefaultImplementation
					 */
					@SuppressWarnings("unchecked")
					OpType op = (OpType) m_factory.newInstance(
						new Class<?>[] {Class.class, NamedParameterJdbcTemplate.class, Object.class},
						new Object[] {m_opClass, npJdbcTemplate, defaultInput},
						CallbackIndices.getCallbacksWithoutDelegate()
					);
					
					return op;
				}
				
				case ABSTRACT: {
					/* Create default implementation delegate */
					final OperationDefaultImplementation opDefaultImpl =
						new OperationDefaultImplementation(m_opClass, npJdbcTemplate, defaultInput);

					/* Create new instance using the no-args constructor */
					@SuppressWarnings("unchecked")
					OpType op = (OpType) m_factory.newInstance(
						new Class<?>[] {},
						new Object[] {},
						CallbackIndices.getCallbacksWithDelegate(opDefaultImpl)
					);
					
					/* Make the actual instance available to the
					 * default implementation delegate
					 */
					opDefaultImpl.m_operation = op;
					
					return op;
				}
				
				case INSTANTIABLE: {
					/* Create new instance using the constructor
					 * (NamedParameterJdbcTemplate, DefaultInput)
					 */
					return m_constructor.newInstance(npJdbcTemplate, defaultInput);
				}
				
				default:
					throw new RuntimeException("unhandled operation class flavour " + m_opClassFlavour);
			}
		}
		catch (Throwable e) {
			throw new InvalidOperationExecutionException("unable to instantiate operation", m_opClass, e);
		}
	}
}
