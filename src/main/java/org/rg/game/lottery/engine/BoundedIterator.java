package org.rg.game.lottery.engine;

import java.util.Iterator;
import java.util.List;

class BoundedIterator implements Iterator<Integer> {
	private List<Integer> numbers;
	private Integer leftBound;
	private Integer rightBound;
	private Integer currentIndex;

	public BoundedIterator(List<Integer> numbers, Integer leftBound, Integer rightBound) {
		this.numbers = numbers;
		this.leftBound = leftBound;
		this.rightBound = rightBound;
		currentIndex = 0;
	}

	@Override
	public boolean hasNext() {
		for (int i = currentIndex; i < numbers.size(); i++) {
			Integer currentNumber = numbers.get(i);
			if (currentNumber >= leftBound && currentNumber <= rightBound) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Integer next() {
		for (int i = currentIndex; i < numbers.size(); i++) {
			Integer currentNumber = numbers.get(i);
			currentIndex++;
			if (currentNumber >= leftBound && currentNumber <= rightBound) {
				return currentNumber;
			}
		}
		return null;
	}

}