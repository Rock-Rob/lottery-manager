package org.rg.game.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.burningwave.Throwables;

public class MathUtils {

	public static final MathUtils INSTANCE = new MathUtils();

	public final DecimalFormat decimalFormat = new DecimalFormat( "#,##0.##" );
	public final DecimalFormat integerFormat = new DecimalFormat( "#,##0" );

	public Comparator<Number> numberComparator = (numberOne, numberTwo) -> {
		double numberOneAsDouble = numberOne.doubleValue();
		double numberTwoAsDouble = numberTwo.doubleValue();
		return numberOneAsDouble > numberTwoAsDouble ? 1 :
			numberOneAsDouble < numberTwoAsDouble ? -1 : 0;
	};

	public DecimalFormat getNewDecimalFormat() {
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		symbols.setGroupingSeparator(',');
		symbols.setDecimalSeparator('.');
		DecimalFormat decimalFormat = new DecimalFormat("#.#", symbols);
		decimalFormat.setParseBigDecimal(true);
		return decimalFormat;
	}

	public BigDecimal stringToBigDecimal(String value) {
		return stringToBigDecimal(value, getNewDecimalFormat());
	}

	public BigDecimal stringToBigDecimal(String value, DecimalFormat decimalFormat) {
		value =	value.trim();
		if (value.contains(".")) {
			String wholeNumber = value.substring(0, value.indexOf("."));
			String fractionalPart = value.substring(value.indexOf(".") + 1, value.length());
			fractionalPart = fractionalPart.replace(".", "");
			value = wholeNumber + "." + fractionalPart;
		}
		try {
			return ((BigDecimal)decimalFormat.parse(value));
		} catch (ParseException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}


	public BigInteger factorial(BigInteger number) {
		return Factorial.of(number).get();
	}

	public BigInteger factorial(String number) {
		return factorial(new BigInteger(number));
	}

	public BigInteger factorial(long number) {
		return factorial(BigInteger.valueOf(number));
	}

	public BigInteger factorial(int number) {
		return factorial(BigInteger.valueOf(number));
	}

	public double sumOfNaturalNumbersBetween(double a, double b) {
		return ((a + b)/2)*((b-a)+1);
	}

	public int sumOfNaturalNumbersBetween(int a, int b) {
		return (int)sumOfNaturalNumbersBetween((double)a, (double)b);
	}

	public BigInteger random(BigInteger upperLimit) {
		Random randomSource = new Random();
		BigInteger number;
		do {
			number = new BigInteger(upperLimit.bitLength(), randomSource);
		} while (number.compareTo(upperLimit) >= 0);
		return number;
	}


	public BigInteger factorial(Number number) {
		return factorial(BigInteger.valueOf(number.longValue()));
	}

	public String format(Number value) {
		if (value == null) {
			return "null";
		}
		return String.format(Locale.ITALY, "%,d", value);
	}

	public static class Factorial {
		private final static Map<String, Factorial> CACHE = new HashMap<>();

		private final static BigInteger loggerStartingThreshold = BigInteger.valueOf(200000);
		BigInteger factorial;
		BigInteger initialValue;
		BigInteger number;
		boolean computed;

		private Factorial(BigInteger number) {
			initialValue = number;
			this.number = number;
		}

		public static Factorial of(BigInteger number) {
			String numberAsString = number.toString();
			Factorial factorial = CACHE.get(numberAsString);
			if (factorial == null) {
				synchronized (CACHE) {
					factorial = CACHE.get(numberAsString);
					if (factorial == null) {
						CACHE.put(numberAsString, factorial = new Factorial(number));
					}
				}
			}
			return factorial;
		}

		Factorial startLogging() {
			CompletableFuture.runAsync(() -> {
				while (!computed) {
					ConcurrentUtils.INSTANCE.sleep(10000);
					if (computed) {
						continue;
					}
					BigInteger processedNumbers = initialValue.subtract(number);
					LogUtils.INSTANCE.info(
						"Processed " + processedNumbers
						.toString() + " numbers - Factorial: " + factorial.toString()
					);
				}
			});
			return this;
		}

		public static BigInteger inverse(BigInteger factorial){
		    BigInteger current = BigInteger.ONE;
		    while (factorial.compareTo(current) > 0) {
		        factorial = factorial.divide(current);
		        current = current.add(BigInteger.ONE);
		    }
		    if (current.compareTo(factorial) == 0) {
		        return current;
		    }
		    return null;
		}

		public BigInteger get() {
			if (computed) {
				return factorial;
			}
			synchronized(this) {
				if (computed) {
					return factorial;
				}
				if (number.compareTo(loggerStartingThreshold) >= 0) {
					startLogging();
				}
				factorial = BigInteger.ONE;
				while (number.compareTo(BigInteger.ZERO) > 0) {
					factorial = factorial.multiply(number);
					number = number.subtract(BigInteger.ONE);
				}
				computed = true;
				return factorial;
			}
		}

	}

}
