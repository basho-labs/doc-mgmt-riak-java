package com.basho.proserv.documentstore;

import com.basho.riak.client.cap.Mutation;

public class DocumentListMerger implements Mutation<DocumentList> {
    private final DocumentList newList;

    public DocumentListMerger(DocumentList newList) {
        this.newList = newList;
    }

    /**
     * @see com.basho.riak.client.cap.Mutation#apply(java.lang.Object)
     */
    public DocumentList apply(DocumentList original) {
        if (original == null) {
            return newList;
        }
        for (String item : original) {
            newList.add(item);
        }
        return newList;
    }

}