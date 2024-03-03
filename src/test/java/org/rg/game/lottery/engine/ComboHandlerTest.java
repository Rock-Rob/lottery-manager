package org.rg.game.lottery.engine;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.rg.game.core.MathUtils;

public class ComboHandlerTest {

	public static class ComboHandlerEnhanced extends ComboHandler {

		public ComboHandlerEnhanced(List<Integer> numbers, long combinationSize) {
			super(numbers, combinationSize);
		}

		protected List<Integer> computeComboFromCounter(BigInteger counter) {
			ComboHandler comboHandler = new ComboHandler(SEStats.NUMBERS, 6);
			BigInteger position = comboHandler.computeCounter(
				Arrays.asList(40,51,53,56,68,72)
			);
			List<Integer> combo = comboHandler.computeCombo(position);
			System.out.println(toString(combo));

			int[] indexes = new int[(int)combinationSize];
			int numbersCount = numbers.size();
			BigInteger diff = getSize().subtract(counter);
			//diff = ((90 - a)!/(6!*(((90 - a)-6)!)))+((90 - b)!/(5!*(((90 - b)-5)!)))+((90 - c)!/(4!*(((90 - c)-4)!)))+((90 - d)!/(3!*(((90 - d)-3)!)))+((90 - e)!/(2!*(((90 - e)-2)!)))+((90 - f)!/(1!*(((90 - f)-1)!)))
			//diff = (a!/(6!*((a-6)!)))+(b!/(5!*((b-5)!)))+(c!/(4!*((c-4)!)))+(d!/(3!*((d-3)!)))+(e!/(2!*((e-2)!)))+(f!/(1!*((f-1)!)))

			//620938055
			for (int i = 0; i < indexes.length; i++) {
				diff = diff.multiply(
					MathUtils.INSTANCE.factorial(
						combinationSize - i
					).multiply(
						MathUtils.INSTANCE.factorial(numbersCount - combinationSize - i)
					)
				);
				indexes[i] = numbersCount - MathUtils.Factorial.inverse(
					diff
				).intValue();
//				counter = counter.subtract(
//					ComboHandler.sizeOf(
//						BigInteger.valueOf(numbersSize - (indexes[i] + 1)),
//						combinationSize - i
//					)
//				);
			}
			return Arrays.stream(indexes).boxed().collect(Collectors.toList());
		}

	}

	public static void main(String[] args) {
		//87 86	85 83
		//88 87 86 84
		//int[] combo = {1, 2, 3, 4};
		List<Integer> combo = Arrays.asList(25, 36, 45, 54, 63, 72);
		//int[] combo = {1, 26, 56, 75};
		ComboHandler sECmbh = new ComboHandler(SEStats.NUMBERS, combo.size());
		BigInteger index = sECmbh.computeCounter(combo);

		List<Integer> cmb = sECmbh.computeCombo(index);
		sECmbh.iterate(iterationData -> {
			if (iterationData.getCounter().longValue() == index.longValue()) {
				System.out.println(
					iterationData.getCombo().stream()
		            .map(Object::toString)
		            .collect(Collectors.joining(", "))
		        );
			}
		});
	}

	public static double computeIncrementation(double y, double n, double m) {
		return
			((m - n + 1)*
			(Math.pow(m, 2) + (m * (n + 2)) +
			Math.pow(n, 2) + n - ((3 *(y - 1)) * y)))/6;
	}

	public static int computeIncrementation(int y, int m, int n) {
		return (int)computeIncrementation((double)y, (double)m, (double)n);
	}
}
