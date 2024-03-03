package org.rg.game.lottery.engine;

public class TerminateIteration extends RuntimeException {
	private static final long serialVersionUID = 4182825598193659018L;

	public static final TerminateIteration NOTIFICATION;
	public static final TerminateIteration ONLY_FOR_THE_CURRENT_THREAD_NOTIFICATION;

	static {
		NOTIFICATION = new TerminateIteration();
		ONLY_FOR_THE_CURRENT_THREAD_NOTIFICATION = new TerminateIteration();
	}

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

}