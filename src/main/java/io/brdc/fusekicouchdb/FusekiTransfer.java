package io.brdc.fusekicouchdb;

import java.util.StringTokenizer;

import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;
import org.slf4j.LoggerFactory;

public class FusekiTransfer {
	static String VERSION =  TransferHelpers.class.getPackage().getImplementationVersion();

	static String fusekiHost = "localhost";
	static String fusekiPort = "13180";
	static String couchdbHost = "localhost";
	static String couchdbPort = "13598";
	static String couchdbName = "bdrc";
	static String fusekiName = "bdrcrw";
	static int howMany = Integer.MAX_VALUE;
	static boolean transferAllDB = false;
	static boolean listenToChanges = true;
	
	private static void printHelp() {
		System.err.print("java -jar FusekiTransfer.jar OPTIONS\n"
				+ "Synchronize couchdb JSON-LD documents with fuseki\n"
				+ "Options:\n" 
				+ "-fusekiHost <host> - host fuseki is running on. Defaults to localhost\n"
				+ "-fusekiPort <port> - port fuseki is running on. Defaults to 13180\n"
				+ "-fusekiName <name> - name of the fuseki endpoint. Defaults to 'bdrcrw'\n"
				+ "-couchdbHost <host> - host couchdb is running on. Defaults to localhost\n"
				+ "-couchdbPort <port> - port couchdb is running on. Defaults to 13598\n"
				+ "-couchdbName <name> - name of the couchdb database. Defaults to 'bdrc'\n"
				+ "-transferAllDB - transfer the whole database\n"
				+ "-doNotListen - do not listen to changes\n"
				+ "-n <int> - specify how many docs to transfer. Defaults to all of the docs\n"
				+ "-progress - enables progress output during transfer\n"
				+ "-debug - enables DEBUG log level - mostly jena logging\n"
				+ "-trace - enables TRACE log level - mostly jena logging\n"
				+ "-help - print this message and exits\n"
				+ "-version - prints the version and exits\n"
				+ "\nset log level with the VM argument -Dorg.slf4j.simpleLogger.defaultLogLevel=XXX\n"
				+ "\nFusekiTransfer version: " + VERSION + "\n"
				);
	}

	public static void main(String[] args) {
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-fusekiHost")) {
				fusekiHost = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-fusekiPort")) {
				fusekiPort = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-fusekiName")) {
				fusekiName = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-couchdbHost")) {
				couchdbHost = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-couchdbPort")) {
				couchdbPort = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-couchdbName")) {
				couchdbName = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-n")) {
				howMany = (++i < args.length ? Integer.parseInt(args[i]) : null);
			} else if (arg.equals("-transferAllDB")) {
				transferAllDB = true;
			} else if (arg.equals("-doNotListen")) {
				listenToChanges = false;
			} else if (arg.equals("-progress")) {
		        TransferHelpers.progress = true;
			} else if (arg.equals("-debug")) {
		        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
		        TransferHelpers.logger = LoggerFactory.getLogger("fuseki-couchdb");
			} else if (arg.equals("-trace")) {
		        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
		        TransferHelpers.logger = LoggerFactory.getLogger("fuseki-couchdb");
			} else if (arg.equals("-help")) {
				printHelp();
				System.exit(0);
			} else if (arg.equals("-version")) {
				System.err.println("FusekiTransfer version: " + VERSION);

				if (TransferHelpers.logger.isDebugEnabled()) {
					System.err.println("Current java.library.path:");
					String property = System.getProperty("java.library.path");
					StringTokenizer parser = new StringTokenizer(property, ";");
					while (parser.hasMoreTokens()) {
						System.err.println(parser.nextToken());
					}
				}

				System.exit(0);
			}
		}
		
		try {
			TransferHelpers.init(fusekiHost, fusekiPort, couchdbHost, couchdbPort, couchdbName, fusekiName);
		} catch (Exception e) {
			TransferHelpers.logger.error("error in initialization", e);
			System.exit(1);
		}

		if (transferAllDB) {		
			try {
				TransferHelpers.transferOntology(null); // use ontology from jar
				TransferHelpers.transferCompleteDB(howMany);
			} catch (Exception ex) {
				TransferHelpers.logger.error("error in complete transfer", ex);
				System.exit(1);
			}
		}
		
		if (listenToChanges) {
			String lastFusekiSequence = TransferHelpers.getLastFusekiSequence();
			
			TransferHelpers.logger.info("listening to couchdb changes...");
			
			ChangesCommand cmd = new ChangesCommand.Builder()
					.includeDocs(true)
	                .continuous(true)
	                .since(lastFusekiSequence)
	                .heartbeat(5000)
					.build();
	
			ChangesFeed feed = TransferHelpers.db.changesFeed(cmd);
	
			while (feed.isAlive()) {
			    DocumentChange change;
			    String id;
				try {
					change = feed.next();
					id = change.getId();
				} catch (InterruptedException e) {
					TransferHelpers.logger.error("error while listening to changes, quitting", e);
					System.exit(1);
					return;
				}
				try {
					TransferHelpers.transferChange(change);
				} catch (Exception e) {
					TransferHelpers.logger.error("error transfering doc "+id, e);
					System.exit(1);
				}
			}
		}
		
		TransferHelpers.executor.shutdown();

	}
}
