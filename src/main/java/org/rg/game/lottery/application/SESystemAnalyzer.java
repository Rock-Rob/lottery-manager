package org.rg.game.lottery.application;

import java.io.IOException;

import org.rg.game.core.FirestoreWrapper;
import org.rg.game.lottery.engine.ComboHandler;
import org.rg.game.lottery.engine.SEStats;

public class SESystemAnalyzer {

	public static void main(String[] args) throws IOException {
		System.out.println(
			ComboHandler.sizeOf(
				ComboHandler.sizeOf(SEStats.NUMBERS.size(), 6), 7
			)
		);
		FirestoreWrapper.shutdownDefaultInstance();
	}

}
