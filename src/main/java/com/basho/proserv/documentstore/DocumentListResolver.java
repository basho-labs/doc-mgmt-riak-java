package com.basho.proserv.documentstore;

import java.util.Collection;
import java.util.HashSet;

import com.basho.riak.client.cap.ConflictResolver;

/**
 * A simple example of a conflict resolver for the DocumentList domain type.
 * Merge the contents of any siblings.
 */
public final class DocumentListResolver implements ConflictResolver<DocumentList> {

    public DocumentList resolve(Collection<DocumentList> siblings) {
        String indexKey = null;
        final Collection<String> items = new HashSet<String>();

        for (DocumentList list : siblings) {
            indexKey = list.key;
            for (String key : list) {
                items.add(key);
            }
        }

        DocumentList resolved = new DocumentList();
        resolved.setKey(indexKey);
        resolved.addAll(items);
        return resolved;
    }
}