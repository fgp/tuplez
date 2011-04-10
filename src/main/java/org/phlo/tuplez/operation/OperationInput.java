package org.phlo.tuplez.operation;

/**
 * Specifies a {@link Operation}'s {@literal InputType}.
 * <p>
 * Don't use this interface directly, use {@link Operation}
 * instead.
 * 
 * @see Operation
 *
 * @param <InputType> the operation's {@literal InputType}
 */
public interface OperationInput<InputType> {
	public boolean getInputSet();
	
	public InputType getInput();

	public void setInput(final InputType input);
	
	public void clearInput();

}
