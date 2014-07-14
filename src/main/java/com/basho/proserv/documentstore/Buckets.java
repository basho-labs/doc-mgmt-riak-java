package com.basho.proserv.documentstore;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.DomainBucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.*;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.JSONConverter;

// Storing the buckets as constants in their own class allows devs to keep track
// of all of them, in once place.
// The alternative is to have a static BUCKET_NAME variable on each class (Document, Metadata, etc)
public class Buckets {
    public static final String DOCUMENTS = "documents";
    public static final String DOC_METADATA = "doc-metadata";
    public static final String ORG_IDX = "org-idx";
    public static final String ACTIVITY_LOG = "activity-log";

    // Used for 2i queries (DomainBuckets don't have that functionality)
    public static Bucket getDocumentMetadataBasicBucket(IRiakClient client) throws RiakRetryFailedException {
        return client.fetchBucket(Buckets.DOC_METADATA)
                .lazyLoadBucketProperties()  // Don't load the properties from the server
                .execute();
    }

    // DomainBucket used to read and write DocumentMetadata objects
    public static DomainBucket<DocumentMetadata> getDocumentMetadataBucket(IRiakClient client) throws RiakRetryFailedException {
        Bucket bucket = Buckets.getDocumentMetadataBasicBucket(client);
        // A DomainBucket wraps a regular bucket, and stores retry, conversion and conflict resolution logic
        // For the DocumentMetadata bucket, don't specify a resolver or mutator - those are only needed when siblings are on
        return DomainBucket.builder(bucket, DocumentMetadata.class)
                .retrier(new DefaultRetrier(3))   // Retry a read/write operation 3 (default, shown here as explicit example)
                .withConverter(new JSONConverter<DocumentMetadata>(DocumentMetadata.class, Buckets.DOC_METADATA))  // default
                .build();
    }

    // Since the Documents bucket only reads and writes simple byte[] data, it does not need custom converters or domain behavior
    public static Bucket getDocumentsBucket(IRiakClient client) throws RiakRetryFailedException {
        return client.fetchBucket(Buckets.DOCUMENTS)
                .lazyLoadBucketProperties()  // Don't load the properties from the server
                .execute();
//        // A DomainBucket wraps a regular bucket, and stores retry, conversion and conflict resolution logic
//        // For the Document bucket, don't specify a resolver or mutator - those are only needed when siblings are on
//        return DomainBucket.builder(bucket, Document.class)
//                .retrier(new DefaultRetrier(3))   // Retry a read/write operation 3 times before throwing exception
//                .withConverter(new Converter<Document>() {
//                    // Convert from a RiakObject to a domain object (Document)
//                    public Document toDomain(IRiakObject riakObject) throws ConversionException {
//                        Document document = new Document();
//                        document.setId(riakObject.getKey());
//                        document.setContents(riakObject.getValue());
//                        return document;
//                    }
//                    // Convert from a Document to a RiakObject
//                    public IRiakObject fromDomain(Document document, VClock vclock) throws ConversionException {
//                        return RiakObjectBuilder.newBuilder(Buckets.DOCUMENTS, document.getId())
//                                .withValue(document.getContents())
//                                .build();
//                    }
//                })
//                .build();
    }

    public static DomainBucket<DocumentList> getOrgIndexBucket(IRiakClient client) throws RiakRetryFailedException {
        Bucket bucket = client.fetchBucket(Buckets.ORG_IDX)
                .lazyLoadBucketProperties()  // Don't load the properties from the server
                .execute();

        // A DomainBucket wraps a regular bucket, and stores retry, conversion and conflict resolution logic
        // For the Org index bucket, don't specify a resolver or mutator - those are only needed when siblings are on
        return DomainBucket.builder(bucket, DocumentList.class)
                .retrier(new DefaultRetrier(3))   // Retry a read/write operation 3 (default, shown here as explicit example)
                .withConverter(new JSONConverter<DocumentList>(DocumentList.class, Buckets.ORG_IDX))  // default
                .mutationProducer(new MutationProducer<DocumentList>() {
                    public Mutation<DocumentList> produce(DocumentList o) {
                        return new DocumentListMerger(o);
                    }
                })
                .withResolver(new DocumentListResolver())
                .build();
    }

    // DomainBucket used to read and write DocumentMetadata objects
    public static DomainBucket<ActivityLog> getActivityLogBucket(IRiakClient client) throws RiakRetryFailedException {
        Bucket bucket = client.fetchBucket(Buckets.ACTIVITY_LOG)
                .lazyLoadBucketProperties()  // Don't load the properties from the server
                .execute();
        // A DomainBucket wraps a regular bucket, and stores retry, conversion and conflict resolution logic
        // For the DocumentMetadata bucket, don't specify a resolver or mutator - those are only needed when siblings are on
        return DomainBucket.builder(bucket, ActivityLog.class)
                .retrier(new DefaultRetrier(3))   // Retry a read/write operation 3 (default, shown here as explicit example)
                .withConverter(new JSONConverter<ActivityLog>(ActivityLog.class, Buckets.ACTIVITY_LOG))  // default
                .build();
    }

    // Initializes bucket properties (for use in "init db" scripts)
    // Makes "Set Bucket Property" calls to the Riak cluster
    // {@see http://docs.basho.com/riak/latest/dev/references/protocol-buffers/set-bucket-props/ }
    public static void initAllBuckets(IRiakClient client) throws RiakRetryFailedException {
        Buckets.initDocumentsBucket(client);
        Buckets.initDocumentMetadataBucket(client);
        Buckets.initOrgIndexBucket(client);
        Buckets.initActivityLogBucket(client);
    }

    public static void initActivityLogBucket(IRiakClient client) throws RiakRetryFailedException {
        client.createBucket(Buckets.ACTIVITY_LOG)
                .allowSiblings(true)
                .execute();
    }

    public static void initDocumentMetadataBucket(IRiakClient client) throws RiakRetryFailedException {
        client.createBucket(Buckets.DOC_METADATA)
                .w(3)   // Write quorum of 3, to improve 2i query consistency
                .allowSiblings(false)
                .execute();
    }

    public static void initOrgIndexBucket(IRiakClient client) throws RiakRetryFailedException {
        client.createBucket(Buckets.ORG_IDX)
                .allowSiblings(true)
                .execute();
    }

    // Set the properties of the Documents bucket (makes a call to the Riak cluster)
    // To be used in "Init new Database" scripts, or in beforeSuite() method of test suites
    public static void initDocumentsBucket(IRiakClient client) throws RiakRetryFailedException {
        client.createBucket(Buckets.DOCUMENTS)
                .allowSiblings(false)
                .execute();
    }
}