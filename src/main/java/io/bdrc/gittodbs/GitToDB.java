package io.bdrc.gittodbs;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class GitToDB {
    
    public static Logger logger = LoggerFactory.getLogger(GitToDB.class);

    static String VERSION =  TransferHelpers.class.getPackage().getImplementationVersion();

	static String fusekiHost = "localhost";
	static String fusekiPort = "13180";
	static String fusekiName = "corerw";
	static String fusekiAuthName = "authrw";
    static String gitDir = null;
    static String ontRoot = "https://raw.githubusercontent.com/buda-base/owl-schema/master/";
	static String libOutputDir = null;
	static boolean transferFuseki = false;
	static boolean exportLib = false;
	static int howMany = Integer.MAX_VALUE;
    static boolean transferAllDB = false;
    static boolean transferOnto = false;
    static boolean listenToChanges = true;
    static boolean connectPerTransfer = false;
    static boolean force = false;
    static boolean ric = false;
	
	static TransferHelpers.DocType docType = null;
	
	private static void printHelp() {
		System.err.print("java -jar GitToDB.jar OPTIONS\n"
		        + "Synchronize couchdb JSON-LD documents with fuseki\n"
		        + "Options:\n" 
		        + "-fuseki             - do transfer to Fuseki\n"
		        + "-fusekiHost <host>  - host fuseki is running on. Defaults to localhost\n"
		        + "-fusekiPort <port>  - port fuseki is running on. Defaults to 13180\n"
                + "-fusekiName <name>  - name of the fuseki endpoint. Defaults to 'corerw'\n"
                + "-fusekiAuthName <name>  - name of the auth fuseki endpoint. Defaults to 'authrw'\n"
                + "-ric                - makes sure data that is restricted in China doesn't reach the Fuseki\n"
                + "-connectPerTransfer - connect to Fuseki for each transfer. Default is connect once per execution\n"
		        + "-libOutputDir       - Output directory of the lib format files\n"
                + "-type <typeName>    - name of the type to transfer: person, item, place, work, topic, lineage, office, product, etext, corporation, etextcontent\n"
		        + "-force              - Transfer all documents if git and distant revisions don't match\n"
                + "-gitDir <path>      - path to the git directory\n"
                + "-ontRoot <path>     - path to the ontology dir. Defaults to GH:buda-base/owl-schema/master/\n"
		        + "-transferOnto       - transfer the core ontology in Fuseki\n"
                + "-timeout <int>      - specify how seconds to wait for a doc transfer to complete. Defaults to 15 seconds\n"
                + "-n <int>            - specify how many resources to transfer; for testing. Default MaxInt\n"
                + "-bulkSz <int>       - specify how many triples to transfer in a bulk transaction. Default 50000\n"
                + "-progress           - enables progress output during transfer\n"
		        + "-debug              - enables DEBUG log level - mostly jena logging\n"
		        + "-trace              - enables TRACE log level - mostly jena logging\n"
		        + "-help               - print this message and exits\n"
		        + "-version            - prints the version and exits\n"
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
	                logger.warn("Pool did not terminate");
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
            } else if (arg.equals("-fusekiAuthName")) {
                fusekiAuthName = (++i < args.length ? args[i] : null);
                transferFuseki = true;
            } else if (arg.equals("-fuseki")) {
                transferFuseki = true;
            } else if (arg.equals("-connectPerTransfer")) {
                connectPerTransfer = true;
            } else if (arg.equals("-type")) {
                String typeName = (++i < args.length ? args[i] : null);
                docType  = TransferHelpers.DocType.getType(typeName);
            } else if (arg.equals("-libOutputDir")) {
                libOutputDir = (++i < args.length ? args[i] : null);
                exportLib = true;
            } else if (arg.equals("-gitDir")) {
                gitDir = (++i < args.length ? args[i] : null);
            } else if (arg.equals("-ontRoot")) {
                ontRoot = (++i < args.length ? args[i] : null);
            } else if (arg.equals("-n")) {
                howMany = (++i < args.length ? Integer.parseInt(args[i]) : null);
            } else if (arg.equals("-bulkSz")) {
                FusekiHelpers.initialLoadBulkSize = (++i < args.length ? Integer.parseInt(args[i]) : null);
			} else if (arg.equals("-timeout")) {
				TransferHelpers.TRANSFER_TO = (++i < args.length ? Integer.parseInt(args[i]) : null);
            } else if (arg.equals("-transferOnto")) {
                transferOnto = true;
            } else if (arg.equals("-ric")) {
                ric = true;
            } else if (arg.equals("-force")) {
                force = true;
            } else if (arg.equals("-progress")) {
                TransferHelpers.progress = true;
            } else if (arg.equals("-debug")) {
                org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
                logger4j.setLevel(org.apache.log4j.Level.toLevel("DEBUG"));
                logger = LoggerFactory.getLogger(GitToDB.class);
            } else if (arg.equals("-trace")) {
                org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
                logger4j.setLevel(org.apache.log4j.Level.toLevel("TRACE"));
                logger = LoggerFactory.getLogger(GitToDB.class);
			} else if (arg.equals("-help")) {
				printHelp();
				System.exit(0);
			} else if (arg.equals("-version")) {
				System.err.println("FusekiTransfer version: " + VERSION);

				if (logger.isDebugEnabled()) {
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
		
		FusekiHelpers.printUsage("INITIAL USAGE  ");
		
		if (!exportLib && !transferFuseki) {
		    logger.error("nothing to do, quitting...");
            System.exit(1);
		}

        if (gitDir == null || gitDir.isEmpty()) {
            logger.error("please specify the git directory");
            System.exit(1);
        }
        
        if (!gitDir.endsWith("/"))
            gitDir+='/';
        
        if (!ontRoot.endsWith("/"))
            ontRoot+='/';
		
        GitHelpers.init();
        
		try {
			TransferHelpers.init();
		} catch (Exception e) {
			logger.error("error in initialization", e);
			System.exit(1);
		}

        if (transferOnto) {
            TransferHelpers.transferOntology(); // use ontology from jar
        }
        
        if (transferFuseki) {
            if (docType != null) {
                try {
                    if (ric &&(docType == DocType.EINSTANCE || docType == DocType.ETEXTCONTENT)) {
                        // we always transfer einstances + etextcontent for security reason when
                        // we're in ric mode
                        int nbLeft = howMany;
                        nbLeft = nbLeft - TransferHelpers.syncType(DocType.EINSTANCE, nbLeft);
                        nbLeft = nbLeft - TransferHelpers.syncType(DocType.ETEXTCONTENT, nbLeft);
                    } else {
                        TransferHelpers.syncType(docType, howMany);
                    }
                    TransferHelpers.closeConnections();
                } catch (Exception ex) {
                    logger.error("error transfering " + docType, ex);
                    System.exit(1);
                }
            } else {
                try {
                    TransferHelpers.sync(howMany);
                } catch (Exception ex) {
                    logger.error("error incomplete transfer", ex);
                    System.exit(1);
                }
            }
    
    		logger.info("FusekiTranser shutting down");
    		shutdownAndAwaitTermination(TransferHelpers.executor);
        }
        if (exportLib) {
            try {
                LibFormat.exportAll();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
		
		// for an unknown reason, when execution reaches this point, if there is not
		// an explicit exit the program simply hangs indefinitely?!
		System.exit(0);
	}
}
