package org.rg.game.lottery.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.rg.game.core.LogUtils;
import org.rg.game.core.MathUtils;

public class SEPremium {
	private final static Map<Number, Integer> NO_WINNING = new TreeMap<>(MathUtils.INSTANCE.numberComparator);
	private final static Map<Integer, Map<Number, Map<Number, Integer>>> WINNING_FOR_INTEGRAL_SYSTEMS = new ConcurrentHashMap<>();

	public static Map<Number, Integer> checkIntegral(List<Integer> combo, List<Integer> winningComboWithJollyAndSuperstar) {
		List<Integer> winningCombo = winningComboWithJollyAndSuperstar.subList(0, 6);
		Number winningType = 0;
		for (Integer number : combo) {
			if (winningCombo.contains(number)) {
				winningType = winningType.intValue() + 1;
			}
		}
		if (winningType.intValue() < 2) {
			return NO_WINNING;
		}
		Integer jolly = winningComboWithJollyAndSuperstar.get(6);
		if (winningType.intValue() > 4 && combo.contains(jolly)) {
			winningType = winningType.doubleValue() + (Premium.TYPE_SIX.doubleValue() - Premium.TYPE_FIVE_PLUS.doubleValue());
		}
		Map<Number, Integer> results = WINNING_FOR_INTEGRAL_SYSTEMS
			.computeIfAbsent(combo.size(), size -> new HashMap<>())
			.computeIfAbsent(winningType, wT -> new TreeMap<>(MathUtils.INSTANCE.numberComparator));
		if (results.isEmpty()) {
			synchronized (results) {
				if (results.isEmpty()) {
					LogUtils.INSTANCE.info("Caching winning type " + winningType + " for combo with size " + combo.size());
					new ComboHandler(combo, 6).iterate(iterationData -> {
						List<Integer> cmb = iterationData.getCombo();
						Number hitCounter = 0;
						for (Integer number : winningCombo) {
							if (cmb.contains(number)) {
								hitCounter = hitCounter.intValue() + 1;
							}
						}
						if (hitCounter.intValue() >= Premium.TYPE_TWO.intValue()) {
							if (hitCounter.intValue() == 5 && cmb.contains(jolly)) {
								results.put(Premium.TYPE_FIVE_PLUS, results.computeIfAbsent(Premium.TYPE_FIVE_PLUS, ht -> 0) + 1);
							} else {
								results.put(hitCounter, results.computeIfAbsent(hitCounter, ht -> 0) + 1);
							}
						}
					});
				}
			}
		}
		return results;
	}

}
