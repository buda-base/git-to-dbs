package io.bdrc.gittodbs;

import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

public class GitToDB {
	static String VERSION =  TransferHelpers.class.getPackage().getImplementationVersion();

	static String fusekiHost = "localhost";
	static String fusekiPort = "13180";
	static String couchdbHost = "localhost";
	static String couchdbPort = "13598";
	static String couchdbName = "bdrc";
	static String fusekiName = "bdrcrw";
	static String gitDir = null;
	static boolean transferFuseki = false;
	static boolean transferCouch = false;
	static int howMany = Integer.MAX_VALUE;
    static boolean transferAllDB = false;
    static boolean transferOnto = false;
	static boolean listenToChanges = true;
	
	private static void printHelp() {
		System.err.print("java -jar GitToDB.jar OPTIONS\n"
				+ "Synchronize couchdb JSON-LD documents with fuseki\n"
				+ "Options:\n" 
				+ "-fusekiHost <host> - host fuseki is running on. Defaults to localhost\n"
				+ "-fusekiPort <port> - port fuseki is running on. Defaults to 13180\n"
				+ "-fusekiName <name> - name of the fuseki endpoint. Defaults to 'bdrcrw'\n"
				+ "-couchdbHost <host> - host couchdb is running on. Defaults to localhost\n"
				+ "-couchdbPort <port> - port couchdb is running on. Defaults to 13598\n"
				+ "-gitDir <path> - path to the git directory\n"
                + "-transferOnto - transfer the core ontology in Fuseki\n"
				+ "-n <int> - specify how many docs to transfer. Defaults to all of the docs\n"
				+ "-timeout <int> - specify how seconds to wait for a doc transfer to complete. Defaults to 15 seconds\n"
				+ "-progress - enables progress output during transfer\n"
				+ "-debug - enables DEBUG log level - mostly jena logging\n"
				+ "-trace - enables TRACE log level - mostly jena logging\n"
				+ "-help - print this message and exits\n"
				+ "-version - prints the version and exits\n"
				+ "\nset log level with the VM argument -Dorg.slf4j.simpleLogger.defaultLogLevel=XXX\n"
				+ "\nFusekiTransfer version: " + VERSION + "\n"
				);
	}

	/*
	 * This is taken from the javadocs for ExecutorService trying in vain to get the
	 * process to actually terminate, but this isn't the issue. Apparently there is a
	 * thread somewhere that is hanging the process and it isn't in the 
	 * ExecutorService.newCachedThreadPool(). I know this because I put a shutdownNow
	 * after this routine is called and logged the number of straggler threads from
	 * the shutdownNow and it was always zero, and yet, the program still didn't terminate!
	 * 
	 * This is left here to prod investigation whenever one of us gets terribly bored -;)
	 */
	static void shutdownAndAwaitTermination(ExecutorService pool) {
	    pool.shutdown(); // Disable new tasks from being submitted
	    try {
	        // Wait a while for existing tasks to terminate
	        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
	            pool.shutdownNow(); // Cancel currently executing tasks
	            // Wait a while for tasks to respond to being cancelled
	            if (!pool.awaitTermination(60, TimeUnit.SECONDS))
	                TransferHelpers.logger.warn("Pool did not terminate");
	        }
	    } catch (InterruptedException ie) {
	        // (Re-)Cancel if current thread also interrupted
	        pool.shutdownNow();
	        // Preserve interrupt status
	        Thread.currentThread().interrupt();
	    }
	}

	public static void main(String[] args) {
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-fusekiHost")) {
				fusekiHost = (++i < args.length ? args[i] : null);
				transferFuseki = true;
			} else if (arg.equals("-fusekiPort")) {
				fusekiPort = (++i < args.length ? args[i] : null);
				transferFuseki = true;
			} else if (arg.equals("-fusekiName")) {
				fusekiName = (++i < args.length ? args[i] : null);
				transferFuseki = true;
			} else if (arg.equals("-couchdbHost")) {
				couchdbHost = (++i < args.length ? args[i] : null);
				transferCouch = true;
			} else if (arg.equals("-couchdbPort")) {
				couchdbPort = (++i < args.length ? args[i] : null);
				transferCouch = true;
            } else if (arg.equals("-gitDir")) {
                gitDir = (++i < args.length ? args[i] : null);
			} else if (arg.equals("-n")) {
				howMany = (++i < args.length ? Integer.parseInt(args[i]) : null);
			} else if (arg.equals("-timeout")) {
				TransferHelpers.TRANSFER_TO = (++i < args.length ? Integer.parseInt(args[i]) : null);
            } else if (arg.equals("-transferOnto")) {
                transferOnto = true;
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
		
		if (!transferCouch && !transferFuseki) {
		    TransferHelpers.logger.error("nothing to do, quitting...");
            System.exit(1);
		}

        if (gitDir == null || gitDir.isEmpty()) {
            TransferHelpers.logger.error("please specify the git directory");
            System.exit(1);
        }
        
        if (!gitDir.endsWith("/"))
            gitDir+='/';
		
		try {
			TransferHelpers.init();
		} catch (Exception e) {
			TransferHelpers.logger.error("error in initialization", e);
			System.exit(1);
		}

        if (transferOnto) {
            TransferHelpers.transferOntology(); // use ontology from jar
        }

        if (transferAllDB) {		
			try {
				TransferHelpers.sync(howMany);
			} catch (Exception ex) {
				TransferHelpers.logger.error("error in complete transfer", ex);
				System.exit(1);
			}
		}

		TransferHelpers.logger.info("FusekiTranser shutting down");
		shutdownAndAwaitTermination(TransferHelpers.executor);
        TransferHelpers.logger.info("FusekiTranser " + couchdbName + " done");
		
		// for an unknown reason, when execution reaches this point, if there is not
		// an explicit exit the program simply hangs indefinitely?!
		System.exit(0);
	}
}
