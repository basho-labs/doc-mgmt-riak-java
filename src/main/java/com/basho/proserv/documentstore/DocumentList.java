package com.basho.proserv.documentstore;

import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.reflect.AnnotationHelper;

import java.util.Collection;
import java.util.HashSet;
import java.util.Arrays;

/**
 * Used to store unique lists of Document keys for use with various security indexes.
 * For example, the Org Index stores sets of document ids that an organization has visibility to
 * This class provides two main functions:
 * 1. Reading and writing of lists of keys into riak (plus set-union sibling resolution rules)
 * 2. Helper methods to multi-get all of the documents for a given set of keys
 */
public class DocumentList extends HashSet<String> {
    @RiakKey
    public String key;

    public DocumentList() {
        super();  // empty set
    }

    public DocumentList(Collection<? extends String> collection) {
        super(collection);
    }

    public DocumentList(String[] keys) {
        super(Arrays.asList(keys));
    }

    public void setKey(String key) {
        this.key = key;
    }
}