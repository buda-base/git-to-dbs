package io.brdc.fusekicouchdb;

import java.util.StringTokenizer;

public class FusekiTransfer {
	static String VERSION = "1.0.3";	
	public static String fusekiHost = "localhost";
	public static String fusekiPort = "13180";
	public static String couchdbHost = "localhost";
	public static String couchdbPort = "13598";
	static boolean debug = false;

	public FusekiTransfer() {
		// TODO Auto-generated constructor stub
	}

	private static void printHelp() {
		System.err.print("java -jar FusekiTransfer.jar OPTIONS\r\n"
				+ "Transfers all JSONLD documents in couchdb to fuseki\r\n"
				+ "Options:\r\n" 
				+ "-fusekiHost <host> - host fuseki is running on. Defaults to localhost\r\n"
				+ "-fusekiPort <port> - port fuseki is running on. Defaults to 13180\r\n"
				+ "-couchdbHost <host> - host couchdb is running on. Defaults to localhost\r\n"
				+ "-couchdbPort <port> - port couchdb is running on. Defaults to 13598\r\n"
				+ "-debug - if present then much more additional information is displayed during execution.\r\n"
				+ "-help - print this message and exits\r\n"
				+ "-version - prints the version and exits\r\n"
				+ "\r\nFusekiTransfer version: " + VERSION + "\r\n"
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

		try {
			
			TransferHelpers.init(fusekiHost, fusekiPort, couchdbHost, couchdbPort);

			TransferHelpers.transferOntology(null); // use ontology from jar
			TransferHelpers.transferCompleteDB();
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
