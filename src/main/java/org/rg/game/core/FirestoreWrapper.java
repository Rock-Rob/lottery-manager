package org.rg.game.core;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.burningwave.Throwables;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;

public class FirestoreWrapper {
	private static FirestoreWrapper DEFAULT_INSTANCE;
	Firestore firestoreClient;

	public FirestoreWrapper(String prfx) throws IOException {
		String prefix = prfx != null && !prfx.isEmpty()? prfx + "." : "";
		String firebaseUrl = System.getenv().getOrDefault(prefix + "firebase.url", System.getenv().get(prefix.toUpperCase().replace(".", "_") + "FIREBASE_URL"));
		if (firebaseUrl == null) {
			throw new NoSuchElementException("Firebase URL not set");
		}
		LogUtils.INSTANCE.info("Database URL " + firebaseUrl);
		InputStream serviceAccount;
		try {
			serviceAccount = new ByteArrayInputStream(
				System.getenv().getOrDefault(
					prefix + "firebase.credentials",
					System.getenv().get(prefix.toUpperCase().replace(".", "_") + "FIREBASE_CREDENTIALS")
				).getBytes());
			LogUtils.INSTANCE.info("Credentials loaded from firebase.credentials");
		} catch (Throwable exc) {
			String credentialsFilePath =
				Paths.get(
					Optional.ofNullable(System.getenv().get(prefix + "firebase.credentials.file"))
						.orElseGet(() -> System.getenv().get(prefix.toUpperCase().replace(".", "_") + "FIREBASE_CREDENTIALS_FILE"))
				).normalize().toAbsolutePath().toString();
			serviceAccount =
				new FileInputStream(
					credentialsFilePath
				);
			LogUtils.INSTANCE.info("Credentials loaded from " + credentialsFilePath);
		}
		FirebaseOptions options = FirebaseOptions.builder()
			  .setCredentials(com.google.auth.oauth2.GoogleCredentials.fromStream(serviceAccount))
			  .setDatabaseUrl(firebaseUrl)
			  .build();

		FirebaseApp.initializeApp(options);
		firestoreClient = FirestoreClient.getFirestore();
	}

	public static FirestoreWrapper get() {
		try {
			if (DEFAULT_INSTANCE == null) {
				synchronized(FirestoreWrapper.class) {
					if (DEFAULT_INSTANCE == null) {
						try {
							DEFAULT_INSTANCE = new FirestoreWrapper(null);
						} catch (IOException exc) {
							return Throwables.INSTANCE.throwException(exc);
						}
					}
				}
			}
		} catch (NoSuchElementException exc) {
			LogUtils.INSTANCE.info(exc.getMessage());
		} catch (Throwable exc) {
			LogUtils.INSTANCE.error(exc, "Unable to connect to Firebase");
		}
		return DEFAULT_INSTANCE;
	}

	public DocumentSnapshot load(String key) {
		DocumentReference recordAsDocumentWrapper = firestoreClient.document(key);
		ApiFuture<DocumentSnapshot> ap = recordAsDocumentWrapper.get();
		try {
			return ap.get();
		} catch (Throwable exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	public void write(String key, Map<String, Object> values) {
		DocumentReference recordAsDocumentWrapper = firestoreClient.document(key);
		try {
			recordAsDocumentWrapper.set(values).get();
			LogUtils.INSTANCE.info("Object with id '" + key + "' stored in the Firebase cache");
		} catch (InterruptedException | ExecutionException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
	}

	public void shutdown() {
		if (firestoreClient != null) {
			firestoreClient.shutdown();
		}
	}

	public static void shutdownDefaultInstance() {
		if (DEFAULT_INSTANCE != null) {
			synchronized(FirestoreWrapper.class) {
				if (DEFAULT_INSTANCE != null) {
					DEFAULT_INSTANCE.shutdown();
					DEFAULT_INSTANCE = null;
				}
			}
		}
	}


}
