package org.rg.game.lottery.engine;

import java.util.Map;
import java.util.function.Predicate;

public class PredicateExpressionParser<I> extends org.burningwave.PredicateExpressionParser<I> {

	@Override
	protected String preProcess(String expression, Object... additionalParamters) {
		return super.preProcess(expression, additionalParamters);
	}

	@Override
	protected Predicate<I> processComplex(String expression, Object... additionalParamters) {
		return super.processComplex(expression, additionalParamters);
	}

	protected static String findAndReplaceNextBracketArea(String expression, Map<String, Object> values) {
		return org.burningwave.PredicateExpressionParser.findAndReplaceNextBracketArea(expression, values);
	}

}
