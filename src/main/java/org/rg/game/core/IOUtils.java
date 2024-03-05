package org.rg.game.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Base64;

import org.burningwave.Throwables;

import com.fasterxml.jackson.databind.ObjectMapper;

public class IOUtils {
	public static final IOUtils INSTANCE = new IOUtils();
	final ObjectMapper objectMapper = new ObjectMapper();

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void copy(InputStream input, OutputStream output) {
		try {
			byte[] buffer = new byte[1024];
			int bytesRead = 0;
			while (-1 != (bytesRead = input.read(buffer))) {
				output.write(buffer, 0, bytesRead);
			}
		} catch (IOException e) {
			Throwables.INSTANCE.throwException(e);
		}
	}

	public File writeToNewFile(String absolutePath, String value) {
		try (FileChannel outChan = new FileOutputStream(absolutePath, true).getChannel()) {
		  outChan.truncate(0);
		} catch (IOException exc) {
			//LogUtils.INSTANCE.error(exc);
		}
		try (FileWriter fileWriter = new FileWriter(absolutePath, false);) {
			fileWriter.write(value);
			fileWriter.flush();
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		return new File(absolutePath);
	}

	public <S extends Serializable> S serializeAndDecode(String serializedAndEncodedObject) {
		try {
			return deserialize(Base64.getDecoder().decode(serializedAndEncodedObject));
		} catch (IOException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public String serializeAndEncode(Object object) {
		return new String (
			Base64.getEncoder().encode(
				serialize(object)
			)
		);
	}

	public byte[] serialize(Object object) {
		try (ByteArrayOutputStream bAOS = new ByteArrayOutputStream(); ObjectOutputStream oOS = new ObjectOutputStream(bAOS);) {
	        oOS.writeObject(object);
	        return bAOS.toByteArray();
		} catch (IOException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public <S extends Serializable> S deserialize(byte[] serializedObject) throws IOException {
		try (ByteArrayInputStream bAIS = new ByteArrayInputStream(serializedObject); ObjectInputStream oIS = new ObjectInputStream(bAIS);) {
	        return (S)oIS.readObject();
		} catch (ClassNotFoundException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public String fileToString(String absolutePath, Charset encoding) {
		return new String(fileContent(absolutePath), encoding);
	}

	public byte[] fileContent(String absolutePath) {
		try {
			return Files.readAllBytes(Paths.get(absolutePath));
		} catch (NoSuchFileException exc) {
			return null;
		} catch (IOException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}

	}

	public void store(String basePath, String key, Serializable object) {
		try (
			FileOutputStream fout = new FileOutputStream(basePath + "/" + key /*Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8))*/ + ".ser");
			ObjectOutputStream oos = new ObjectOutputStream(fout)
		) {
			oos.writeObject(object);
			LogUtils.INSTANCE.info("Object with id '" + key + "' stored in the local cache");
		} catch (Throwable exc) {
			Throwables.INSTANCE.throwException(exc);
		}
	}

	public <T extends Serializable> T load(String basePath, String key) {
		try (FileInputStream fIS = new FileInputStream(basePath + "/" + key /*Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8))*/ + ".ser");
			ObjectInputStream oIS = new ObjectInputStream(fIS)) {
			T effectiveItem = (T) oIS.readObject();
			//LogUtils.INSTANCE.info("Object with id '" + key + "' loaded from physical cache" /*+ ": " + effectiveItem*/);
	        return effectiveItem;
		} catch (FileNotFoundException exc) {
			return null;
		} catch (Throwable exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public <T> T readFromJSONFormat(File jsonFile, Class<T> cls) {
		try {
			if (jsonFile.exists()) {
				return objectMapper.readValue(jsonFile, cls);
			}
			return null;
		} catch (IOException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public <T> T readFromJSONFormat(String jsonContent, Class<T> cls) {
		try {
			return objectMapper.readValue(jsonContent, cls);
		} catch (IOException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public void writeToJSONFormat(File jsonFile, Object object) {
		try {
			objectMapper.writeValue(jsonFile, object);
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
	}

	public String writeToJSONFormat(Object object) {
		try {
			return objectMapper.writeValueAsString(object);
		} catch (IOException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public void writeToJSONPrettyFormat(File jsonFile, Object object) {
		try {
			objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, object);
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
	}

}
