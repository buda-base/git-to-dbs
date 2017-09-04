package io.bdrc.gittodbs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.IndexDiff;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.util.io.DisabledOutputStream;

import io.bdrc.gittodbs.TransferHelpers.DocType;

public class GitHelpers {
    public static Map<DocType,Repository> typeRepo = new EnumMap<>(DocType.class);
    
    public static Ref refFromRev(DocType type, String rev) {
        Repository r = typeRepo.get(type); 
        if (r == null)
            return null;
        return r.
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
}
