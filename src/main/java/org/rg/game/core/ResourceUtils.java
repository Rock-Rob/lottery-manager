package org.rg.game.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.burningwave.Throwables;
import org.rg.game.lottery.engine.PersistentStorage;

public class ResourceUtils {
	public static final ResourceUtils INSTANCE = new ResourceUtils();

	public String getExtension(File file) {
		return file.getName().substring(file.getName().lastIndexOf(".") + 1, file.getName().length());
	}

	public List<File> find(FilenameFilter filter) {
		return toOrderedList(getResourceFolder().listFiles(filter));
	}

	public File getResourceFolder() {
		try {
			File resourceFolder = Paths.get(IOUtils.class.getResource("/" +
				IOUtils.class.getName().replace(".", "/") + ".class"
			).toURI()).toFile();
			for (String pathSegment : IOUtils.class.getName().split("\\.")) {
				resourceFolder = resourceFolder.getParentFile();
			}
			return resourceFolder;
		} catch (URISyntaxException e) {
			return Throwables.INSTANCE.throwException(e);
		}
	}

	public File getResource(String path) {
		File file = getResourceFolder();
		if (path != null && !path.isEmpty()) {
			file = new File(file.getAbsolutePath() + File.separator + path);
		}
		if (file.exists()) {
			return file;
		}
		return null;
	}

	public List<File> findOrdered(String filePrefix, String extension, String... paths) {
		return toOrderedList(find(filePrefix, extension, paths), false);
	}

	public List<File> findReverseOrdered(String filePrefix, String extension, String... paths) {
		return toOrderedList(find(filePrefix, extension, paths), true);
	}

	public List<File> find(String filePrefix, String extension, String... paths) {
		Collection<File> files = new TreeSet<>(buildFileComparator());
		for (String path : paths) {
			files.addAll(
				Arrays.asList(
					new File(path).listFiles((directory, fileName) ->
						fileName.contains(filePrefix) && fileName.endsWith(extension)
					)
				)
			);
		}
		return toOrderedList(files);
	}


	public List<Properties> toOrderedProperties(List<File> files) throws IOException {
		List<Properties> propertiesColl = new ArrayList<>();
		for (int i = 0; i < files.size(); i++) {
			Properties properties = toProperties(files.get(i));
			if (properties.getProperty("priority-of-this-configuration") == null) {
				properties.setProperty("priority-of-this-configuration", String.valueOf((files.size() - 1) - i));
			}
			propertiesColl.add(properties);
		}
		Comparator<Properties> comparator = (propsOne, propsTwo) -> {
			BigDecimal propsOnePriority =
					MathUtils.INSTANCE.stringToBigDecimal(propsOne.getProperty("priority-of-this-configuration"));
				BigDecimal propsTwoPriority =
					MathUtils.INSTANCE.stringToBigDecimal(propsTwo.getProperty("priority-of-this-configuration"));
				return propsOnePriority.compareTo(propsTwoPriority);
		};
		Collections.sort(propertiesColl, comparator.reversed());
		return propertiesColl;
	}

	public Properties toProperties(File file) throws IOException {
		try (InputStream configIS = new FileInputStream(file)) {
			Properties properties = new Properties() {

				private static final long serialVersionUID = -5098290977439105868L;

				@Override
				public String toString() {
					String priority = getProperty("priority-of-this-configuration");
					if (priority != null) {
						return getProperty("file.name") + " - priority: " + priority;
					}
					return super.toString();
				}
			};

			properties.load(configIS);
			properties.setProperty("file.name", file.getName());
			properties.setProperty("file.parent.absolutePath", file.getParentFile().getAbsolutePath());
			properties.setProperty("file.extension", ResourceUtils.INSTANCE.getExtension(file));
			String baseProps = properties.getProperty("base");
			if (baseProps != null) {
				Properties merged = new Properties();
				for (String base : baseProps.split(";")) {
					merged.putAll(
						toProperties(
							toFile(file.getParentFile().getAbsolutePath(), base)
						)
					);
				}
				merged.putAll(properties);
				properties = merged;
				properties.remove("base");
			}
			return properties;
		}
	}

	public File toFile(String parentAbsolutePath, String childPath) {
		if (!(childPath.startsWith("./") || childPath.startsWith("../"))) {
			childPath = "./" + childPath;
		}
		return Paths.get(parentAbsolutePath + File.separator + childPath).normalize().toFile();
	}

	public File backup(
		File mainFile,
		String destFolderAbsolutePath
	) {
		return backup(TimeUtils.now(), mainFile, destFolderAbsolutePath);
	}

	public File backup(
		LocalDateTime backupTime,
		File mainFile,
		String destFolderAbsolutePath
	) {
		try {
			String backupFileName = mainFile.getName().replace("." +  getExtension(mainFile), "") + " - [" + TimeUtils.dateTimeFormatForBackup.format(backupTime) + "]." + getExtension(mainFile);
			String backupFilePath = destFolderAbsolutePath + File.separator + backupFileName;
			File backupFile = new File(backupFilePath);
			if (!backupFile.exists()) {
				backupFile.getParentFile().mkdirs();
				try (InputStream inputStream = new FileInputStream(mainFile); OutputStream backupOutputStream = new FileOutputStream(backupFilePath)) {
					IOUtils.INSTANCE.copy(
						inputStream,
						backupOutputStream
					);
				}
				return new File(backupFilePath);
			}
			return null;
		} catch (IOException e) {
			return Throwables.INSTANCE.throwException(e);
		}
	}

	public List<File> toOrderedList(Object files, boolean reversed) {
		Set<File> orderedFiles = new TreeSet<>(reversed ?  buildFileComparator().reversed() :  buildFileComparator());
		if (files.getClass().isArray()) {
			orderedFiles.addAll(Arrays.asList((File[])files));
		} else if (files instanceof Collection) {
			orderedFiles.addAll((Collection<File>)files);
		}
		return new ArrayList<>(orderedFiles);
	}

	private Comparator<File> buildFileComparator() {
		Comparator<File> fileComparator = (fileOne, fileTwo) -> {
			int fileNameComparator = fileOne.getName().compareTo(fileTwo.getName());
			if (fileNameComparator == 0) {
				return fileOne.getAbsolutePath().compareTo(fileTwo.getAbsolutePath());
			}
			return fileNameComparator;
		};
		return fileComparator;
	}

	public List<File> toOrderedList(Object files) {
		return toOrderedList(files, false);
	}

	public String[] pathsFromSystemEnv(String... keys) {
		return pathsFromSystemEnv(null, keys);
	}

	public String[] pathsFromSystemEnv(List<String> values, String... keys) {
		if (values == null) {
			values = new ArrayList<>();
		}
		for (String key : keys) {
			String value = CollectionUtils.INSTANCE.retrieveValue(key);
			if (value != null) {
				if (key.startsWith("working-path.") || key.startsWith("WORKING_PATH_")) {
					values.add(PersistentStorage.buildWorkingPath(value));
				} else if (key.startsWith("resources.") || key.startsWith("RESOURCES_")) {
					values.add(ResourceUtils.INSTANCE.getResource(value).getAbsolutePath());
				} else {
					File file = new File(value);
					if (file.exists()) {
						values.add(file.getAbsolutePath());
					}
				}
			}
		}
		return values.stream().toArray(String[]::new);
	}

}
