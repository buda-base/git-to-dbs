package io.brdc.fusekicouchdb;

import java.util.StringTokenizer;

import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;

public class FusekiTransfer {
	static String VERSION =  TransferHelpers.class.getPackage().getImplementationVersion();

	public static String fusekiHost = "localhost";
	public static String fusekiPort = "13180";
	public static String couchdbHost = "localhost";
	public static String couchdbPort = "13598";
	public static int howMany = Integer.MAX_VALUE;
	static boolean debug = false;
	static boolean transferAllDB = false;
	static boolean listenToChanges = true;
	
	private static void printHelp() {
		System.err.print("java -jar FusekiTransfer.jar OPTIONS\n"
				+ "Synchronize couchdb JSON-LD documents with fuseki\n"
				+ "Options:\n" 
				+ "-fusekiHost <host> - host fuseki is running on. Defaults to localhost\n"
				+ "-fusekiPort <port> - port fuseki is running on. Defaults to 13180\n"
				+ "-couchdbHost <host> - host couchdb is running on. Defaults to localhost\n"
				+ "-couchdbPort <port> - port couchdb is running on. Defaults to 13598\n"
				+ "-transferAllDB - transfer the whole database\n"
				+ "-doNotListen - do not listen to changes\n"
				+ "-n <int> - specify how many docs to transfer. Defaults to all of the docs\n"
				+ "-debug - if present then much more additional information is displayed during execution.\n"
				+ "-help - print this message and exits\n"
				+ "-version - prints the version and exits\n"
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
			} else if (arg.equals("-couchdbHost")) {
				couchdbHost = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-couchdbPort")) {
				couchdbPort = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-n")) {
				howMany = (++i < args.length ? Integer.parseInt(args[i]) : null);
			} else if (arg.equals("-transferAllDB")) {
				transferAllDB = true;
			} else if (arg.equals("-doNotListen")) {
				listenToChanges = false;
			} else if (arg.equals("-help")) {
				printHelp();
				System.exit(0);
			} else if (arg.equals("-debug")) {
				debug = true;;
			} else if (arg.equals("-version")) {
				System.err.println("FusekiTransfer version: " + VERSION);

				if (debug) {
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
		
		TransferHelpers.init(fusekiHost, fusekiPort, couchdbHost, couchdbPort, debug);

		if (transferAllDB) {		
			try {
				TransferHelpers.transferOntology(null); // use ontology from jar
				TransferHelpers.transferCompleteDB(howMany);
			
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
		if (listenToChanges) {
			String lastFusekiSequence = TransferHelpers.getLastFusekiSequence();
			
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
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
				try {
					TransferHelpers.transferChange(change);
				} catch (Exception e) {
					System.err.println("error transfering doc "+id);
					e.printStackTrace();
				}
			}
		}
		
	}
}
