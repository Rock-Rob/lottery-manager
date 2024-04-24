package org.rg.game.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.burningwave.Synchronizer;
import org.burningwave.Throwables;
import org.rg.game.lottery.engine.ComboHandler;
import org.rg.game.lottery.engine.Premium;
import org.rg.game.lottery.engine.Storage;

public class PersistentStorage implements Storage {
	private static String workingPath;
	BufferedWriter bufferedWriter = null;
	String absolutePath;
	String parentPath;
	String name;
	int size;
	Boolean isClosed;
	Map<Integer, Integer> occurrences;
	Map<Number, Integer> historicalPremiums;

	public PersistentStorage(
		LocalDate extractionDate,
		int combinationCount,
		int numberOfCombos,
		String group,
		String suffix
	) {
		absolutePath = (parentPath = buildWorkingPath(group)) + File.separator +
			(name = Storage.computeName(extractionDate, combinationCount, numberOfCombos, suffix));
		try (FileChannel outChan = new FileOutputStream(absolutePath, true).getChannel()) {
		  outChan.truncate(0);
		} catch (IOException exc) {
			//LogUtils.INSTANCE.error(exc);
		}
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(absolutePath, false));
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		occurrences = new LinkedHashMap<>();
	}

	private PersistentStorage(String group, String fileName) {
		absolutePath = (parentPath = buildWorkingPath(group)) + File.separator + fileName;
		name = fileName;
		occurrences = new LinkedHashMap<>();
	}

	public static PersistentStorage restore(String group, String fileName) {
		PersistentStorage storage = new PersistentStorage(group, fileName) {
			@Override
			public boolean addCombo(List<Integer> selectedCombo) {
				throw new UnsupportedOperationException(this + " is only readable");
			}
			@Override
			public boolean addLine() {
				throw new UnsupportedOperationException(this + " is only readable");
			}
			@Override
			public boolean addLine(String value) {
				throw new UnsupportedOperationException(this + " is only readable");
			}
			@Override
			public void addUnindexedCombo(List<Integer> selectedCombo) {
				throw new UnsupportedOperationException(this + " is only readable");
			}
		};
		try {
			Iterator<List<Integer>> comboIterator = storage.iterator();
			while (comboIterator.hasNext()) {
				comboIterator.next();
				storage.size++;
			}
		} catch (Throwable exc) {
			if (!(exc instanceof FileNotFoundException)) {
				throw exc;
			}
			return null;
		}
		return storage;
	}

	public static String buildWorkingPath() {
		return buildWorkingPath(null);
	}

	public static String buildWorkingPath(String subFolder) {
		if (workingPath == null) {
			synchronized (PersistentStorage.class) {
				if (workingPath == null) {
					String workingPath = CollectionUtils.INSTANCE.retrieveValue("lottery-util.working-path");
					workingPath =
						workingPath != null ? workingPath :
						System.getProperty("user.home") + File.separator +
						"Desktop" + File.separator +
						"Combos";
					workingPath = Paths.get(workingPath).normalize().toFile().getAbsolutePath();
					LogUtils.INSTANCE.info("Set working path to: " + workingPath);
					PersistentStorage.workingPath = workingPath;
				}
			}
		}
		String absolutePath = workingPath + (subFolder != null? File.separator + subFolder : "");
		File workingFolder = new File(absolutePath);
		workingFolder.mkdirs();
		return absolutePath;
	}

	public String getAbsolutePath() {
		return absolutePath;
	}

	public String getAbsolutePathWithoutExtension() {
		return absolutePath.substring(0, absolutePath.lastIndexOf("."));
	}

	@Override
	public String getName() {
		return name;
	}

	public String getParentPath() {
		return name;
	}


	public String getNameWithoutExtension() {
		return name.substring(0, name.lastIndexOf("."));
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public List<Integer> getCombo(int idx) {
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(absolutePath))){
			String line = null;
			int iterationIndex = 0;
			while ((line = bufferedReader.readLine()) != null) {
				if (line.split("\\t").length > 0) {
					if (iterationIndex == idx) {
						List<Integer> selectedCombo = new ArrayList<>();
						for (String numberAsString : line.split("\\t")) {
							try {
								selectedCombo.add(Integer.parseInt(numberAsString));
							} catch (NumberFormatException exc) {
								return null;
							}
						}
						return selectedCombo;
					}
					iterationIndex++;
				}
			}
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		return null;
	}

	@Override
	@SuppressWarnings("resource")
	public Iterator<List<Integer>> iterator() {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(absolutePath));
			return new Iterator<List<Integer>>() {
				List<Integer> nextCombo = nextCombo();

				@Override
				public List<Integer> next() {
					if (nextCombo == null) {
						throw new NoSuchElementException("No combo available");
					}
					List<Integer> currentCombo = this.nextCombo;
					this.nextCombo = nextCombo();
					if (this.nextCombo == null) {
						close();
					}
					return currentCombo;
				}

				@Override
				public boolean hasNext() {
					if (nextCombo == null) {
						close();
					}
					return nextCombo != null;
				}

				public void close() {
					try {
						bufferedReader.close();
					} catch (IOException exc) {
						Throwables.INSTANCE.throwException(exc);
					}
				}

				private List<Integer> nextCombo() {
					try {
						String line = bufferedReader.readLine();
						if (line != null) {
							List<Integer> selectedCombo = new ArrayList<>();
							for (String numberAsString : line.split("\\t")) {
								try {
									selectedCombo.add(Integer.parseInt(numberAsString));
								} catch (NumberFormatException exc) {
									return null;
								}
							}
							return selectedCombo;
						}
						return null;
					} catch (IOException exc) {
						return Throwables.INSTANCE.throwException(exc);
					}
				}
			};
		} catch (FileNotFoundException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}
/*
	@Override
	public Iterator<List<Integer>> iterator() {
		return new Iterator<List<Integer>>() {
			int currentIndex = 0;
			@Override
			public List<Integer> next() {
				return getCombo(currentIndex++);
			}

			@Override
			public boolean hasNext() {
				return getCombo(currentIndex) != null;
			}
		};
	}*/

	@Override
	public boolean addCombo(List<Integer> combo) {
		if (!contains(combo)) {
			try {
				bufferedWriter.write(ComboHandler.toString(combo) + "\n");
				bufferedWriter.flush();
				++size;
				for (Integer number : combo) {
					Integer counter = occurrences.computeIfAbsent(number, key -> 0) + 1;
					occurrences.put(number, counter);
				}
			} catch (IOException exc) {
				Throwables.INSTANCE.throwException(exc);
			}
			return true;
		}
		return false;
	}

	@Override
	public void addUnindexedCombo(List<Integer> selectedCombo) {
		try {
			bufferedWriter.write("\n" + ComboHandler.toString(selectedCombo));
			bufferedWriter.flush();
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
	}

	public boolean contains(List<Integer> selectedCombo) {
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(absolutePath))){
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				for (String numberAsString : line.split("\\t")) {
					if (!selectedCombo.contains(Integer.parseInt(numberAsString))) {
						return false;
					}
				}
				return true;
			}
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		return false;
	}

	@Override
	public void printAll() {
	    try (BufferedReader br = new BufferedReader(new FileReader(absolutePath))) {
	        String line;
	        while ((line = br.readLine()) != null) {
	           LogUtils.INSTANCE.info(line);
	        }
	    } catch (IOException exc) {
	    	LogUtils.INSTANCE.error(exc);
		}
	 }

	@Override
	public boolean isClosed() {
		if (isClosed != null) {
			return isClosed;
		}
		if (bufferedWriter != null) {
			return false;
		}
		synchronized (this) {
			if (isClosed == null) {
				try (BufferedReader br = new BufferedReader(new FileReader(absolutePath))) {
			        String line;
			        while ((line = br.readLine()) != null) {
			           if (line.contains(END_LINE_PREFIX)) {
			        	   isClosed = true;
			           }
			        }
			    } catch (IOException e) {
					Throwables.INSTANCE.throwException(e);
				}
			}
		}
		return isClosed != null ? isClosed : false;
	}

	@Override
	public void close() {
		try {
			isClosed = true;
			bufferedWriter.close();
		} catch (IOException exc) {
			LogUtils.INSTANCE.error(exc);
		}
	}

	@Override
	public boolean addLine(String value) {
		try {
			bufferedWriter.write("\n" + value);
			bufferedWriter.flush();
		} catch (IOException e) {
			Throwables.INSTANCE.throwException(e);
		}
		return true;
	}

	@Override
	public boolean addLine() {
		try {
			bufferedWriter.write("\n");
			bufferedWriter.flush();
		} catch (IOException e) {
			Throwables.INSTANCE.throwException(e);
		}
		return true;
	}

	@Override
	public void delete() {
		try {
			bufferedWriter.close();
		} catch (IOException e) {
			Throwables.INSTANCE.throwException(e);
		}
		new File(absolutePath).delete();
	}

	@Override
	public Integer getMinOccurence() {
		if (size() == 0) {
			return 0;
		}
		loadOccurrencesIfNeeded();
		TreeSet<Integer> occurrencesCounter = new TreeSet<>(occurrences.values());
		return occurrencesCounter.iterator().next();
	}

	@Override
	public Integer getMaxOccurence() {
		if (size() == 0) {
			return 0;
		}
		loadOccurrencesIfNeeded();
		TreeSet<Integer> occurrencesCounter = new TreeSet<>(occurrences.values());
		return occurrencesCounter.descendingIterator().next();
	}

	private void loadOccurrencesIfNeeded() {
		if (occurrences.isEmpty()) {
			Synchronizer.INSTANCE.execute(absolutePath + "_computeOccurrences", () -> {
				if (occurrences.isEmpty()) {
					Iterator<List<Integer>> comboItr = iterator();
					Map<Integer, Integer> occurrences = new LinkedHashMap<>();
					while (comboItr.hasNext()) {
						for (Integer number : comboItr.next()) {
							Integer counter = occurrences.computeIfAbsent(number, key -> 0) + 1;
							occurrences.put(number, counter);
						}
					}
					this.occurrences = occurrences;
				}
			});
		}
	}

	@Override
	public Map<Number, Integer> getHistoricalPremiums() {
		if (this.historicalPremiums == null) {
			Synchronizer.INSTANCE.execute(absolutePath + "_computeHistoricalPremiums", () -> {
				if (this.historicalPremiums == null) {
					Map<Number, Integer> historicalPremiums = new LinkedHashMap<>();
					try (BufferedReader br = new BufferedReader(new FileReader(absolutePath))) {
				        String line;
				        boolean startToCollect = false;
				        while ((line = br.readLine()) != null) {
				           if (line.contains("Riepilogo risultati storici sistema dal")) {
				        	   startToCollect = true;
				        	   continue;
				           } else if (line.contains("Costo:")) {
				        	  break;
				           }
				           if (startToCollect && !(line = line.replaceAll("\\s+","")).isEmpty()) {
				        	   String[] premiumLabelAndCounter = line.split(":");
				        	   historicalPremiums.put(
			        			   Premium.toType(premiumLabelAndCounter[0]),
			        			   MathUtils.INSTANCE.integerFormat.parse(premiumLabelAndCounter[1]).intValue()
		        			   );
				           }
				        }
				    } catch (Throwable e) {
						Throwables.INSTANCE.throwException(e);
					}
					this.historicalPremiums = historicalPremiums;
				}
			});
		}
		return historicalPremiums;
	}

}
