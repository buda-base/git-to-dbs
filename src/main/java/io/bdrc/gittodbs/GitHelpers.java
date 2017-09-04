package io.bdrc.gittodbs;

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
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.util.io.DisabledOutputStream;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class GitHelpers {
    public static Map<DocType,Repository> typeRepo = new EnumMap<>(DocType.class);
    
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
        String res = rc.getName();
        git.close();
        return res;
    }
    
    public static String getHeadRev(DocType type) {
        Repository r = typeRepo.get(type); 
        if (r == null)
            return null;
        Ref headRef;
        try {
            headRef = r.exactRef(r.getFullBranch());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return headRef.getObjectId().name();
    }
    
    public static List<DiffEntry> getChanges(DocType type, String sinceRev) {
        Repository r = typeRepo.get(type); 
        if (r == null)
            return null;
        ObjectId commitId = ObjectId.fromString(sinceRev);
        RevCommit commit;
        List<DiffEntry> entries;
        try {
            RevWalk walk = new RevWalk(r);
            commit = walk.parseCommit( commitId );
            walk.close();
            CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
            oldTreeIter.reset(r.newObjectReader(), commit);
            OutputStream outputStream = DisabledOutputStream.INSTANCE;
            ObjectId head = r.resolve("HEAD^{tree}");
            CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
            newTreeIter.reset(r.newObjectReader(), head);
            DiffFormatter formatter = new DiffFormatter(outputStream);
            formatter.setRepository(r);
            entries = formatter.scan(oldTreeIter, newTreeIter);
            formatter.close();
        } catch (Exception e) {
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
            e2.printStackTrace();
            return null;
        }

        // a RevWalk allows to walk over commits based on some filtering that is defined
        RevWalk walk = new RevWalk(r);

        RevCommit commit;
        try {
            commit = walk.parseCommit(head.getObjectId());
        } catch (IOException e1) {
            e1.printStackTrace();
            return null;
        }
        RevTree tree = commit.getTree();
        TreeWalk treeWalk = new TreeWalk(r);
        try {
            treeWalk.addTree(tree);
        } catch (IOException e) {
            e.printStackTrace();
        }
        treeWalk.setRecursive(true);
        treeWalk.setFilter(PathFilter.create("*.ttl"));
        return treeWalk;
    }
}
