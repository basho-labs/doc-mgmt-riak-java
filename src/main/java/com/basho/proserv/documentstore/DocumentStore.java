package com.basho.proserv.documentstore;

import com.basho.proserv.documentstore.DocumentMetadata;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.proserv.documentstore.Buckets;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.query.indexes.BinIndex;

import java.util.List;

public class DocumentStore {
    IRiakClient client;

    public DocumentStore(IRiakClient client) {
        this.client = client;
    }

    // Hard-delete the document. (Only use to cleanup orphaned data, admin-only)
    public void deleteDocument(Document document) throws RiakException {
        // TODO: Decide - Should this be logged in Activity log as well?
        this.deleteDocumentMetadata(document.getId());
        this.removeFromOrgIndex(document);
        this.deleteDocumentContents(document.getId());
    }

    public void deleteDocumentContents(String documentId) throws RiakException {
        Buckets.getDocumentsBucket(this.client).delete(documentId).execute();
    }

    public void deleteDocumentMetadata(String documentId) throws RiakException {
        // domain buckets don't need execute()
        Buckets.getDocumentMetadataBucket(this.client).delete(documentId, null);  // note the use of 'null' vclock
    }

    // Return the document contents (if found), null if not found
    public byte[] getDocumentContents(String documentId) throws RiakRetryFailedException {
        Bucket documents = Buckets.getDocumentsBucket(this.client);
        IRiakObject result = documents.fetch(documentId).execute();
        if (result != null) {
            return result.getValue();
        } else {
            return null;  // Object was not found
        }
    }

    // Return the document metadata (if found), null if not found
    public DocumentMetadata getDocumentMetadata(String documentId) throws RiakException {
        DomainBucket<DocumentMetadata> documentMetadataBucket = Buckets.getDocumentMetadataBucket(this.client);
        return documentMetadataBucket.fetch(documentId);  // note that DomainBuckets don't use .execute() to fetch
    }

    public void insertDocument(Document document) throws RiakException {
        this.activityLogStart(document);
        this.insertDocumentContents(document);
        this.addToOrgIndex(document);
        this.storeDocumentMetadata(document);
        this.activityLogEnd(document);
    }

    // Update Document metadata.
    // NOTE: This is a dumb 'update the Metadata json object in riak',
    //  and can't update lists (it doesn't know which lists to remove from and which to add to)
    //  This should be encapsulated as a RetireOperation object (which stores old lists to remove the id from, and new lists to add to)
    public void updateDocument(Document document) throws RiakException {
        // TODO: Decide - Should this be logged in Activity log as well?
        this.storeDocumentMetadata(document);
    }

    public void activityLogEnd(Document document) throws RiakException {
        DomainBucket<ActivityLog> activityLogBucket = Buckets.getActivityLogBucket(this.client);
        ActivityLog activityLog = activityLogBucket.fetch(document.getId()); // This will change once the key changes, and be a lookup through the timeseries boxes
        activityLog.setStatus(ActivityLog.ActivityLogStatus.COMPLETED);
        activityLogBucket.store(activityLog);
    }

    public void activityLogStart(Document document) throws RiakException {
        DomainBucket<ActivityLog> activityLogBucket = Buckets.getActivityLogBucket(this.client);
        ActivityLog activityLog = new ActivityLog();
        activityLog.setActivityLogKey(document.getId()); // these 2 sets are both to document.getId(). The key needs to be changed to something unique
        activityLog.setDocumentId(document.getId()); // these 2 sets are both to document.getId(). The key needs to be changed to something unique
        activityLog.setStatus(ActivityLog.ActivityLogStatus.STARTED);
        activityLogBucket.store(activityLog);
    }

    public ActivityLog getActivityLog(String key) throws RiakException {
        DomainBucket<ActivityLog> activityLogBucket = Buckets.getActivityLogBucket(this.client);
        ActivityLog activityLog = activityLogBucket.fetch(key);
        return activityLog;
    }

    // Insert, no siblings (overwrites old value)
    public void insertDocumentContents(Document document) throws RiakException {
        Bucket documentsBucket = Buckets.getDocumentsBucket(this.client);
        documentsBucket.store(document.getId(), document.getContents()).execute();
    }

    // Insert or Update, no siblings (overwrites old value)
    public void storeDocumentMetadata(Document document) throws RiakException {
        DomainBucket<DocumentMetadata> documentMetadataBucket = Buckets.getDocumentMetadataBucket(this.client);
        documentMetadataBucket.store(document.getMetadata());
    }

    public List<String> queryDocsByOrg(String orgId) throws RiakException {
        Bucket bucket = Buckets.getDocumentMetadataBasicBucket(this.client);
        // Make a 2i query (can only be done by basic buckets)
        List<String> keys = bucket.fetchIndex(BinIndex.named(DocumentMetadata.ORG_INDEX_NAME)).withValue(orgId).execute();
        return keys;
    }

    public void addToOrgIndex(Document document) throws RiakException {
        DomainBucket<DocumentList> bucket = Buckets.getOrgIndexBucket(this.client);
        String indexListKey = document.getMetadata().getOrganizationId();  // e.g. "org123"
        // Fetch the contents of the original list
        DocumentList oldList = bucket.fetch(indexListKey);
        if (oldList == null) {  // no existing list found
            oldList = new DocumentList();  // init to empty set
        }
        oldList.setKey(indexListKey);
        // Add document key to the list
        oldList.add(document.getId());
        // Write it back to Riak
        bucket.store(oldList);
    }

    public DocumentList getOrgIndex(String indexListKey) throws RiakException {
        DomainBucket<DocumentList> bucket = Buckets.getOrgIndexBucket(this.client);
        return bucket.fetch(indexListKey);
    }

    public void removeFromOrgIndex(Document document) throws RiakException {
        DomainBucket<DocumentList> bucket = Buckets.getOrgIndexBucket(this.client);
        String indexListKey = document.getMetadata().getOrganizationId();  // e.g. "org123"
        // Fetch the contents of the original list
        DocumentList oldList = bucket.fetch(indexListKey);
        if (oldList == null) {  // no existing list found
            // if the original list is empty, and you're being asked to remove an item from it,
            // something is wrong.
            // Log exception here
            return;
        }
        oldList.setKey(indexListKey);
        oldList.remove(document.getId());
        // Write it back to Riak
        bucket.store(oldList);
    }
}