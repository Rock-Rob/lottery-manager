package org.rg.game.core;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.plaf.basic.BasicScrollBarUI;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

import org.burningwave.Throwables;
import org.rg.game.lottery.engine.PersistentStorage;

public interface LogUtils {
	public static final LogUtils INSTANCE = retrieveConfiguredLogger();
	public static final DateTimeFormatter dateTimeFormatter =
		DateTimeFormatter.ofPattern("[dd/MM/yyyy-HH:mm:ss.SSS]")
		.withZone(TimeUtils.DEFAULT_TIME_ZONE);
	public final static boolean showThreadInfo = Boolean.parseBoolean(System.getenv().getOrDefault("logger.show-thread-info", "false"));
	static LogUtils retrieveConfiguredLogger() {
		String loggerType = System.getenv().getOrDefault("logger.type", "console");
		if (loggerType.equalsIgnoreCase("console")) {
			return new LogUtils.ToConsole();
		} else if (loggerType.equalsIgnoreCase("file")) {
			return LogUtils.ToFile.getLogger("default-log.txt");
		} else if (loggerType.equalsIgnoreCase("window")) {
			return new LogUtils.ToWindow(
				System.getenv().getOrDefault("logger.window.attached-to", null)
			);
		}
		throw new IllegalArgumentException(loggerType + " is not a valid logger type");
	}

	public default String decorate(String line) {
		String prefix = dateTimeFormatter.format(LocalDateTime.now()) + (showThreadInfo ? " - " + Thread.currentThread() : "") + ": ";
		if (!line.contains("\n")) {
			return prefix + line;
		} else {
			char[] charArray = new char[prefix.length()];
			for (int i = 0; i < charArray.length; i++) {
			    charArray[i] = ' ';
			}
			return prefix + line.replace("\n", "\n" + new String(charArray));
		}
	}


	public void debug(String... reports);

	public void info(String... reports);

	public void warn(String... reports);

	public void error(String... reports);

	public void error(Throwable exc, String... reports);

	public static class ToConsole implements LogUtils {

		@Override
		public void debug(String... reports) {
			log(System.out, reports);
		}

		@Override
		public void info(String... reports) {
			log(System.out, reports);
		}

		@Override
		public void warn(String... reports) {
			log(System.err, reports);
		}

		@Override
		public void error(String... reports) {
			log(System.err, reports);
		}

		@Override
		public void error(Throwable exc, String... reports) {
			if (reports == null || reports.length == 0) {
				System.err.println();
			} else {
				for (String report : reports) {
					System.err.println(decorate(report));
				}
			}
			if (exc.getMessage() != null) {
				System.err.println(decorate(exc.getMessage()));
			}
			for (StackTraceElement stackTraceElement : exc.getStackTrace()) {
				System.err.println(decorate("\t" + stackTraceElement.toString()));
			}
		}

		private void log(PrintStream stream, String... reports) {
			if (reports == null || reports.length == 0) {
				stream.println();
				return;
			}
			for (String report : reports) {
				stream.println(decorate(report));
			}
		}
	}

	public static class ToFile implements LogUtils {
		public final static Map<String, ToFile> INSTANCES = new ConcurrentHashMap<>();
		private BufferedWriter writer;

		private ToFile(String absolutePath) {
			try {
				try (FileChannel outChan = new FileOutputStream(absolutePath, true).getChannel()) {
				  //outChan.truncate(0);
				} catch (IOException exc) {
					Throwables.INSTANCE.throwException(exc);
				}
				writer = new BufferedWriter(new FileWriter(absolutePath, true));
			} catch (IOException exc) {
				Throwables.INSTANCE.throwException(exc);
			}
		}

		public final static LogUtils getLogger(String relativePath) {
			String absolutePath =
				PersistentStorage.buildWorkingPath() + File.separator + (relativePath = relativePath != null? relativePath : "log.txt");
			return INSTANCES.computeIfAbsent(relativePath, key -> new ToFile(absolutePath));
		}

		@Override
		public void debug(String... reports) {
			for (int i = 0; i < reports.length; i++) {
				reports[i] = "[DEBUG] - " + reports[i];
			}
			log(reports);
		}

		@Override
		public void info(String... reports) {
			for (int i = 0; i < reports.length; i++) {
				reports[i] = "[INFO] - " + reports[i];
			}
			log(reports);
		}

		@Override
		public void warn(String... reports) {
			for (int i = 0; i < reports.length; i++) {
				reports[i] = "[WARN] - " + reports[i];
			}
			log(reports);
		}

		@Override
		public void error(String... reports) {
			for (int i = 0; i < reports.length; i++) {
				reports[i] = "[ERROR] - " + reports[i];
			}
			log(reports);
		}

		@Override
		public void error(Throwable exc, String... reports) {
			try {
				if (reports == null || reports.length == 0) {
					writer.append("\n");
				} else {
					for (String report : reports) {
						writer.append(decorate(report + "\n"));
					}
				}
				if (exc.getMessage() != null) {
					writer.append(decorate(exc.getMessage() + "\n"));
				}
				for (StackTraceElement stackTraceElement : exc.getStackTrace()) {
					writer.append(decorate("\t" + stackTraceElement.toString()));
				}
			} catch (Throwable innerExc) {
				Throwables.INSTANCE.throwException(exc);
			}
		}

		private void log(String... reports) {
			try {
				if (reports == null || reports.length == 0) {
					writer.append("\n");
					return;
				}
				for (String report : reports) {
					writer.append(decorate(report) + "\n");
				}
				writer.flush();
			} catch (Throwable exc) {
				Throwables.INSTANCE.throwException(exc);
			}
		}

	}

	public static class ToWindow implements LogUtils {
		private Consumer<String> debugLogger;
		private Consumer<String> infoLogger;
		private Consumer<String> warnLogger;
		private Consumer<String> errorLogger;

		ToWindow(String loggerName) {
			List<Consumer<String>> loggers =
				loggerName != null?
					WindowHandler.attachNewWindowToLogger(loggerName):
					WindowHandler.newWindow();
			debugLogger = loggers.get(0);
			infoLogger = loggers.get(1);
			warnLogger = loggers.get(2);
			errorLogger = loggers.get(3);
		}

		@Override
		public void debug(String... reports) {
			log(debugLogger, reports);
		}

		@Override
		public void info(String... reports) {
			log(infoLogger, reports);
		}

		@Override
		public void warn(String... reports) {
			log(warnLogger, reports);
		}

		@Override
		public void error(String... reports) {
			log(errorLogger, reports);
		}

		@Override
		public void error(Throwable exc, String... reports) {
			if (reports == null || reports.length == 0) {
				errorLogger.accept("\n");
			} else {
				for (String report : reports) {
					errorLogger.accept(decorate(report + "\n"));
				}
			}
			if (exc.getMessage() != null) {
				errorLogger.accept(decorate(exc.getMessage() + "\n"));
			}
			for (StackTraceElement stackTraceElement : exc.getStackTrace()) {
				errorLogger.accept(decorate("\t" + stackTraceElement.toString() + "\n"));
			}
		}

		private void log(Consumer<String> logger, String... reports) {
			if (reports == null || reports.length == 0) {
				logger.accept("\n");
				return;
			}
			for (String report : reports) {
				logger.accept(decorate(report) + "\n");
			}
		}


		private static class WindowHandler extends Handler {
		    private StyledDocument console;
		    private final static SimpleAttributeSet debugTextStyle;
		    private final static SimpleAttributeSet infoTextStyle;
		    private final static SimpleAttributeSet warnTextStyle;
		    private final static SimpleAttributeSet errorTextStyle;
		    private final static Map<String, Color> cachedColors;
			private final static int maxNumberOfCharacters = Integer.valueOf(System.getenv().getOrDefault("logger.window.max-number-of-characters", "524288"));
			private final static String windowLoggerInitialWidth = System.getenv().getOrDefault("logger.window.initial-width", "1024");
			private final static String windowLoggerInitialHeight = System.getenv().getOrDefault("logger.window.initial-height", "576");
			private final static String windowLoggerInitialXPosition = System.getenv().getOrDefault("logger.window.initial-x-position", "60");
			private final static String windowLoggerInitialYPosition = System.getenv().getOrDefault("logger.window.initial-y-position", "60");
			private final static String backgroundColor = System.getenv().getOrDefault("logger.window.background-color", "67,159,54");
			private final static String textColor = System.getenv().getOrDefault("logger.window.text-color", "253,195,17");
			private final static String barBackgroundColor = System.getenv().getOrDefault("logger.window.bar.background-color", "253,195,17");
			private final static String barTextColor = System.getenv().getOrDefault("logger.window.bar.text-color", "67,159,54");

			static {
				cachedColors = new ConcurrentHashMap<>();
				com.formdev.flatlaf.FlatLightLaf.setup();
				JFrame.setDefaultLookAndFeelDecorated(true);
				debugTextStyle = new SimpleAttributeSet();
			    StyleConstants.setForeground(debugTextStyle, Color.BLUE);
			    //StyleConstants.setBackground(debugTextStyle, Color.YELLOW);

			    infoTextStyle = new SimpleAttributeSet();
			    StyleConstants.setForeground(infoTextStyle, Color.WHITE);

			    warnTextStyle = new SimpleAttributeSet();
			    StyleConstants.setForeground(warnTextStyle, Color.YELLOW);
			    StyleConstants.setBold(warnTextStyle, true);

			    errorTextStyle = new SimpleAttributeSet();
			    StyleConstants.setForeground(errorTextStyle, Color.RED);
			    StyleConstants.setBold(errorTextStyle, true);
			}

			private WindowHandler() {
				//LogManager manager = LogManager.getLogManager();
				//String className = this.getClass().getName();
				//String level = manager.getProperty(className + ".level");
				//setLevel(level != null ? Level.parse(level) : Level.ALL);
				setLevel(Level.ALL);
				if (console == null) {					javax.swing.JFrame window = new javax.swing.JFrame(System.getenv().getOrDefault("lottery.application.name", "Event logger")) {
						private static final long serialVersionUID = 653831741693111851L;
						{
							setSize(Integer.valueOf(windowLoggerInitialWidth), Integer.valueOf(windowLoggerInitialHeight));
							setLocation(Integer.valueOf(windowLoggerInitialXPosition), Integer.valueOf(windowLoggerInitialYPosition));
						}
					};
					window.getRootPane().putClientProperty("JRootPane.titleBarForeground", stringToColor(WindowHandler.barTextColor));
					window.getRootPane().putClientProperty("JRootPane.titleBarBackground", stringToColor(WindowHandler.barBackgroundColor));
					window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

					JPanel consoleWrapperWithNoTextWrap = new JPanel(new BorderLayout());
					JScrollPane scrollableConsoleWrapper = new javax.swing.JScrollPane(consoleWrapperWithNoTextWrap);
					scrollableConsoleWrapper.getHorizontalScrollBar().setBackground(stringToColor(WindowHandler.backgroundColor));
					scrollableConsoleWrapper.getVerticalScrollBar().setBackground(stringToColor(WindowHandler.backgroundColor));
					int scrollBarIncrementStep = 8;
					scrollableConsoleWrapper.getVerticalScrollBar().setUnitIncrement(scrollBarIncrementStep);
					scrollableConsoleWrapper.getHorizontalScrollBar().setUnitIncrement(scrollBarIncrementStep);
					scrollableConsoleWrapper.getHorizontalScrollBar().setUI(new BasicScrollBarUI() {
					    @Override
					    protected void configureScrollBarColors() {
					        this.thumbColor = stringToColor(WindowHandler.barBackgroundColor);
					    }
					});
					scrollableConsoleWrapper.getVerticalScrollBar().setUI(new BasicScrollBarUI() {
					    @Override
					    protected void configureScrollBarColors() {
					        this.thumbColor = stringToColor(WindowHandler.barBackgroundColor);
					    }
					});
					window.add(scrollableConsoleWrapper);
					JTextPane consoleWrapper = new JTextPane();					javax.swing.text.DefaultCaret caret = (javax.swing.text.DefaultCaret)consoleWrapper.getCaret();
					caret.setUpdatePolicy(javax.swing.text.DefaultCaret.ALWAYS_UPDATE);
					consoleWrapper.setBorder(
						BorderFactory.createCompoundBorder(
							consoleWrapper.getBorder(),
							BorderFactory.createEmptyBorder(5, 5, 5, 5)
						)
					);

					consoleWrapper.setBackground(stringToColor(WindowHandler.backgroundColor));
					consoleWrapper.setForeground(stringToColor(WindowHandler.textColor));
					consoleWrapper.setFont(new Font(consoleWrapper.getFont().getName(), Font.PLAIN, consoleWrapper.getFont().getSize() + 2));
					consoleWrapper.setEditable(false);
					consoleWrapperWithNoTextWrap.add(consoleWrapper);

					console = consoleWrapper.getStyledDocument();

					window.setVisible(true);
				}
			}

			protected Color stringToColor(String colorAsString) {
				return cachedColors.computeIfAbsent(colorAsString, cAS -> {
					List<Integer> rGBColor =
						Arrays.asList(cAS.split(",")).stream()
						.map(Integer::valueOf).collect(Collectors.toList());
					if (rGBColor.size() == 3) {
						return new Color(rGBColor.get(0), rGBColor.get(1), rGBColor.get(2));
					} else if (rGBColor.size() == 4) {
						return new Color(rGBColor.get(0), rGBColor.get(1), rGBColor.get(2), rGBColor.get(3));
					} else {
						throw new IllegalArgumentException("Unvalid color " + colorAsString);
					}
				});
			}

			public static List<Consumer<String>> attachNewWindowToLogger(String loggerName) {
				WindowHandler WindowHandler = new WindowHandler();
				Logger logger = Logger.getLogger(loggerName);
				logger.addHandler(WindowHandler);
				return Arrays.asList(
					logger::fine,
					logger::info,
					logger::warning,
					logger::severe
				);
			}

			public static List<Consumer<String>> newWindow() {
				WindowHandler WindowHandler = new WindowHandler();
				return Arrays.asList(
					message ->
						WindowHandler.publish(new LogRecord(Level.FINE, message)),
					message ->
						WindowHandler.publish(new LogRecord(Level.INFO, message)),
					message ->
						WindowHandler.publish(new LogRecord(Level.WARNING, message)),
					message ->
						WindowHandler.publish(new LogRecord(Level.SEVERE, message))
				);
			}

			@Override
			public synchronized void publish(LogRecord record) {
				if (!isLoggable(record)) {
					return;
				}
				try {
					if (console.getEndPosition().getOffset() > maxNumberOfCharacters) {
						console.remove(0, maxNumberOfCharacters - 1);
					}
					console.insertString(
						console.getEndPosition().getOffset() -1,
						record.getMessage(),
						getSimpleAttributeSet(record.getLevel())
					);
				} catch (BadLocationException exc) {

				}
			}

			private AttributeSet getSimpleAttributeSet(Level level) {
				if (level == Level.FINE) {
					return debugTextStyle;
				} else if (level == Level.INFO) {
					return infoTextStyle;
				} else if (level == Level.WARNING) {
					return warnTextStyle;
				} else if (level == Level.SEVERE) {
					return errorTextStyle;
				}
				return null;
			}

			@Override
			public void close() {}

			@Override
			public void flush() {}

		}

	}

}
