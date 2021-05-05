package io.bdrc.gittodbs;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.errors.InvalidObjectIdException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.util.io.DisabledOutputStream;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class GitHelpers {
    public static Map<DocType,Repository> typeRepo = new EnumMap<>(DocType.class);
    
    public static void init() {
        ensureGitRepo(DocType.CORPORATION);
        ensureGitRepo(DocType.LINEAGE);
        ensureGitRepo(DocType.OFFICE);
        ensureGitRepo(DocType.PERSON);
        ensureGitRepo(DocType.PLACE);
        ensureGitRepo(DocType.TOPIC);
        ensureGitRepo(DocType.ITEM);
        ensureGitRepo(DocType.WORK);
        ensureGitRepo(DocType.EINSTANCE);
        ensureGitRepo(DocType.IINSTANCE);
        ensureGitRepo(DocType.INSTANCE);
        ensureGitRepo(DocType.ETEXT);
        ensureGitRepo(DocType.ETEXTCONTENT);
        ensureGitRepo(DocType.COLLECTION);
        ensureGitRepo(DocType.SUBSCRIBER);
    }
    
    // for tests only
    public static void createGitRepo(DocType type) {
        String dirpath = GitToDB.gitDir + type + 's';
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        File gitDir = new File(dirpath+"/.git");
        File wtDir = new File(dirpath);
        try {
            Repository repository = builder.setGitDir(gitDir)
              .setWorkTree(wtDir)
              .readEnvironment()
              .build();
            repository.create();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void ensureGitRepo(DocType type) {
        if (typeRepo.containsKey(type))
            return;
        String dirpath = GitToDB.gitDir + type + 's';
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        File gitDir = new File(dirpath+"/.git");
        File wtDir = new File(dirpath);
        try {
            Repository repository = builder.setGitDir(gitDir)
              .setWorkTree(wtDir)
              .setMustExist( true )
              .readEnvironment() // scan environment GIT_* variables
              .build();
            if (!repository.getObjectDatabase().exists()) {
                TransferHelpers.logger.error(dirpath+" does not seem to be a valid git repository, quitting");
                System.exit(1);
            }
            typeRepo.put(type, repository);
        } catch (IOException e) {
            TransferHelpers.logger.error(dirpath+" does not seem to be a valid git repository, quitting");
            System.exit(1);
        }
    }
    
    public static String getLastRefOfFile(DocType type, String path) {
        Repository r = typeRepo.get(type);
        Git git = new Git(r);
        Iterator<RevCommit> commits;
        try {
            commits = git.log().addPath(path).setMaxCount(1).call().iterator();
        } catch (GitAPIException e) {
            e.printStackTrace();
            git.close();
            return null;
        }
        RevCommit rc = commits.next();
        String res = null;
        if (rc != null)
            res = rc.getName();
        git.close();
        return res;
    }
    
    public static boolean hasRev(DocType type, String rev) {
        Repository r = typeRepo.get(type); 
        if (r == null)
            return false;
        try {
            Ref ref = r.findRef(rev);
            return (ref != null);
        } catch (IOException e) {
            return false;
        }
    }
    
    public static String getHeadRev(DocType type) {
        Repository r = typeRepo.get(type); 
        if (r == null)
            return null;
        Ref headRef;
        try {
            headRef = r.exactRef(r.getFullBranch());
        } catch (IOException e) {
            TransferHelpers.logger.error("", e);
            return null;
        }
        if (headRef == null)
            return null;
        return headRef.getObjectId().name();
    }
    
    public static List<DiffEntry> getChanges(DocType type, String sinceRev) throws InvalidObjectIdException, MissingObjectException {
        Repository r = typeRepo.get(type); 
        if (r == null)
            return null;
        ObjectId commitId;
        commitId = ObjectId.fromString(sinceRev);
        RevCommit commit;
        List<DiffEntry> entries = null;
        try {
            RevWalk walk = new RevWalk(r);
            commit = walk.parseCommit( commitId );
            walk.close();
            CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
            oldTreeIter.reset(r.newObjectReader(), commit.getTree());
            OutputStream outputStream = DisabledOutputStream.INSTANCE;
            ObjectId head = r.resolve("HEAD^{tree}");
            CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
            newTreeIter.reset(r.newObjectReader(), head);
            DiffFormatter formatter = new DiffFormatter(outputStream);
            formatter.setRepository(r);
            entries = formatter.scan(oldTreeIter, newTreeIter);
            formatter.close();
        } catch (MissingObjectException e) {
            // oddity due to MissingObjectException inheriting from IOException
            throw new MissingObjectException(commitId, e.getMessage());
        } catch (IOException e) {
            TransferHelpers.logger.error("", e);
            return null;
        }
        return entries;
    }
    
    public static TreeWalk listRepositoryContents(DocType type) {
        Repository r = typeRepo.get(type); 
        if (r == null)
            return null;
        Ref head;
        try {
            head = r.exactRef(r.getFullBranch());
        } catch (IOException e2) {
            TransferHelpers.logger.error("unable to get reference of HEAD", e2);
            return null;
        }

        // a RevWalk allows to walk over commits based on some filtering that is defined
        RevWalk walk = new RevWalk(r);

        RevCommit commit;
        try {
            commit = walk.parseCommit(head.getObjectId());
        } catch (IOException e1) {
            TransferHelpers.logger.error("unable to parse commit, this shouldn't happen", e1);
            walk.close();
            return null;
        }
        walk.close();
        RevTree tree = commit.getTree();
        TreeWalk treeWalk = new TreeWalk(r);
        try {
            treeWalk.addTree(tree);
        } catch (IOException e) {
            TransferHelpers.logger.error("internal error, this shouldn't happen", e);
        }
        treeWalk.setRecursive(true);
        return treeWalk;
    }
}
