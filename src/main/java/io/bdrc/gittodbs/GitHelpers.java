package io.bdrc.gittodbs;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.InvalidObjectIdException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.revwalk.filter.RevFilter;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.util.io.DisabledOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class GitHelpers {
    public static final Map<DocType,Repository> typeRepo = new EnumMap<>(DocType.class);
    public static final String localSuffix = "-20220922";
    public static Logger logger = LoggerFactory.getLogger(GitHelpers.class);
    
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
        ensureGitRepo(DocType.OUTLINE);
        ensureGitRepo(DocType.USER);
        if (!GitToDB.ric)
            ensureGitRepo(DocType.SUBSCRIBER);
    }
    
    // for tests only
    public static void createGitRepo(DocType type) {
        final String dirpath = GitToDB.gitDir + type + 's' + localSuffix;
        final FileRepositoryBuilder builder = new FileRepositoryBuilder();
        final File gitDir = new File(dirpath+"/.git");
        final File wtDir = new File(dirpath);
        try {
            Repository repository = builder.setGitDir(gitDir)
              .setWorkTree(wtDir)
              .readEnvironment()
              .build();
            repository.create();
        } catch (IOException e) {
            logger.error("can't create git repo on "+dirpath, e);
        }
    }
    
    public static void ensureGitRepo(DocType type) {
        if (typeRepo.containsKey(type))
            return;
        final String dirpath = GitToDB.gitDir + type + 's' + localSuffix;
        final FileRepositoryBuilder builder = new FileRepositoryBuilder();
        final File gitDir = new File(dirpath+"/.git");
        final File wtDir = new File(dirpath);
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
            if (type == DocType.USER)
            	typeRepo.put(DocType.USER_PRIVATE, repository);
        } catch (IOException e) {
            logger.error(dirpath+" does not seem to be a valid git repository, quitting");
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
            logger.error("can't get last revision of file " + path, e);
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
            ObjectId commitId = r.resolve(rev);
            return commitId != null;
        } catch (RevisionSyntaxException | IOException e1) {
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
    
    public static List<DiffEntry> getChanges(final DocType type, final String sinceRev) throws InvalidObjectIdException, RevisionSyntaxException, AmbiguousObjectException, IncorrectObjectTypeException, IOException {
        return getChanges(type, sinceRev, "HEAD");
    }
    
    public static List<DiffEntry> getChanges(final DocType type, final String sinceRev, final String toRev) throws InvalidObjectIdException, RevisionSyntaxException, AmbiguousObjectException, IncorrectObjectTypeException, IOException {
        final Repository r = typeRepo.get(type); 
        if (r == null)
            return null;
        List<DiffEntry> entries = null;
        final ObjectId commitId = r.resolve(sinceRev);
        try {
            final ObjectId headCommitId = r.resolve(toRev+"^{commit}");
            final RevWalk walk = new RevWalk(r);
            // not 100% sure but seems to improve things
            walk.setRevFilter(RevFilter.NO_MERGES);
            final RevCommit commit = walk.parseCommit(commitId);
            final RevCommit headCommit = walk.parseCommit(headCommitId);
            walk.close();
            if (headCommit.getCommitTime() < commit.getCommitTime()) {
                logger.error("can't getchanges for type " + type + " since revision " + sinceRev + ", head time < commit time");
                return null;
            }
            logger.info("get changes for type "+type+" from "+sinceRev+" to "+headCommitId.getName());
            CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
            oldTreeIter.reset(r.newObjectReader(), commit.getTree());
            OutputStream outputStream = DisabledOutputStream.INSTANCE;
            CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
            final ObjectId head = r.resolve(toRev+"^{tree}");
            newTreeIter.reset(r.newObjectReader(), head);
            DiffFormatter formatter = new DiffFormatter(outputStream);
            formatter.setRepository(r);
            entries = formatter.scan(oldTreeIter, newTreeIter);
            formatter.close();
        } catch (MissingObjectException e) {
            // oddity due to MissingObjectException inheriting from IOException
            throw new MissingObjectException(commitId, e.getMessage());
        } catch (IOException e) {
            logger.error("can't getchanges for type " + type + " since revision " + sinceRev, e);
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
            logger.error("unable to get reference of HEAD", e2);
            return null;
        }

        // a RevWalk allows to walk over commits based on some filtering that is defined
        RevWalk walk = new RevWalk(r);

        RevCommit commit;
        try {
            commit = walk.parseCommit(head.getObjectId());
        } catch (IOException e1) {
            logger.error("unable to parse commit, this shouldn't happen", e1);
            walk.close();
            return null;
        }
        walk.close();
        RevTree tree = commit.getTree();
        TreeWalk treeWalk = new TreeWalk(r);
        try {
            treeWalk.addTree(tree);
        } catch (IOException e) {
            logger.error("internal error, this shouldn't happen", e);
        }
        treeWalk.setRecursive(true);
        return treeWalk;
    }
}
