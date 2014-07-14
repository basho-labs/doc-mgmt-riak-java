package com.basho.proserv.documentstore;

import java.util.UUID;

import com.basho.riak.client.convert.RiakKey;

public class Document {
    @RiakKey
    public String id;

    private DocumentMetadata metadata;

    private byte[] contents;

    public Document() {
    }

    public Document(String id, DocumentMetadata metadata, byte[] contents) {
        this();
        setId(id);
        setMetadata(metadata);
        setContents(contents);
    }

    // Usage: document.setId(Document.generateId());
    public static String generateId() {
        return UUID.randomUUID().toString();
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DocumentMetadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(DocumentMetadata metadata) {
        this.metadata = metadata;
    }

    public byte[] getContents() {
        return this.contents;
    }

    public void setContents(byte[] contents) {
        this.contents = contents;
    }
}