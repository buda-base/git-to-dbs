package io.bdrc.gittodbs;

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class GitHelpers {
    public static Map<DocType,Repository> typeRepo = new EnumMap<>(DocType.class);
    
    public static void init() {
        ensureGitRepo(DocType.PERSON);
        ensureGitRepo(DocType.ITEM);
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
