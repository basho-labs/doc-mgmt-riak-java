package com.basho.proserv.documentstore;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.basho.riak.client.convert.RiakKey;
import com.basho.riak.client.convert.RiakIndex;

public class DocumentMetadata {
    @RiakKey
    private String documentId;  // @RiakKey annotation required for marshalling

    // user-supplied
    private String title;
    private Date dateCreated;
    private Date expirationDate;
    private String author;
    private String organizationId;

    // potentially inferred from uploaded file
    private String contentType;

    public static final String ORG_INDEX_NAME = "org";

    public DocumentMetadata() {
    }

    public DocumentMetadata(String documentId, String title, String author,
                            String contentType) {
        this();
        setDocumentId(documentId);
        setTitle(title);
        setAuthor(author);
        setContentType(contentType);
    }

    // TODO: Override this properly using EqualsBuilder, also implement hashCode(), using HashcodeBuilder
    public boolean equals(DocumentMetadata other) {
        if (other == null) return false;
        if (!this.getDocumentId().equals(other.getDocumentId())) return false;
        if (!this.getTitle().equals(other.getTitle())) return false;
        // ...
        return true;
    }

    public String getOrganizationId() {
        // Used for creating 2i indices in DocumentStore#storeDocumentMetadata()
        return this.organizationId;
    }

    // Creates a 2i index entry for the Organization index
    @JsonIgnore
    @RiakIndex(name = DocumentMetadata.ORG_INDEX_NAME)
    public Set<String> getOrgIndex() {
        // in the object http header, this will look like:
        // x-riak-index-org_bin: org123
        HashSet<String> indexValue = new HashSet<String>();
        indexValue.add(this.getOrganizationId());
        return indexValue;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Date getDateCreated() {
        return this.dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    public Date getExpirationDate() {
        return this.expirationDate;
    }

    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    @Override
    public String toString() {
        return "DocumentMetadata(" + getDocumentId() + "): " + " " +
                getContentType() + " " +
                getTitle() + " " + getAuthor();
    }
}
