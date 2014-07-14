package com.basho.proserv.documentstore.functional;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.basho.proserv.documentstore.*;
import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.junit.Test;

import static com.basho.proserv.documentstore.functional.TestClient.getTestClient;
import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.IRiakClient;
import org.xml.sax.SAXException;

public class DocumentStoreTest {
    public static Document getTestDocument() {
        Document document = new Document();
        document.setId(Document.generateId());

        try (InputStream fileStream = DocumentStoreTest.class.getResourceAsStream("/testpdf.pdf")) {
            byte[] documentContents = IOUtils.toByteArray(fileStream);

            document.setContents(documentContents);

            DocumentMetadata documentMetadata = new DocumentMetadata(document.getId(),
                    "TestTitle", "TestAuthor",
                    MimeTypeUtility.getRawMimeType(documentContents, "testpdf.pdf"));
            documentMetadata.setOrganizationId("org123");

            document.setMetadata(documentMetadata);
        } catch (IOException e) {
            fail("Exception reading test document: " + e);
        } catch (SAXException e) {
            fail("Exception reading test document: " + e);
        } catch (TikaException e) {
            fail("Exception reading test document: " + e);

        }

        return document;
    }

    @Test
    public void testStoreDocumentContents() throws IOException, InterruptedException, RiakException {
        IRiakClient client = getTestClient();
        DocumentStore api = new DocumentStore(client);

        Document document = getTestDocument();

        String testKey = document.getId();
        byte[] testValue = document.getContents();

        // Store the document contents in Riak
        api.insertDocumentContents(document);

        // Retrieve contents by key, and compare with original
        byte[] fetchedValue = api.getDocumentContents(testKey);
        assertArrayEquals(testValue, fetchedValue);

        // Delete/clean up
        api.deleteDocumentContents(testKey);

        // Make sure the document no longer exists (fetch returns null)
        assertNull(api.getDocumentContents(testKey));
    }

    @Test
    public void testStoreDocumentMetadata() throws IOException, InterruptedException, RiakException {

        IRiakClient client = getTestClient();
        DocumentStore api = new DocumentStore(client);
        Document document = getTestDocument();
        String testKey = document.getId();

//        System.out.println(testKey);

        DocumentMetadata metadata = document.getMetadata();
        String testOrgId = metadata.getOrganizationId();

        // Store the document metadata in Riak
        api.storeDocumentMetadata(document);

        // Retrieve metadata by key, and compare with original
        DocumentMetadata fetchedMetadata = api.getDocumentMetadata(testKey);
        assertTrue(fetchedMetadata.equals(metadata));

        // Now retrieve the key for this document by the 'org' secondary index
        List<String> keys = api.queryDocsByOrg(testOrgId);  // all the doc ids for this org
        assertTrue(keys.contains(testKey));  // Make sure the list of keys includes this one that was just inserted

        // Delete/clean up
        api.deleteDocumentMetadata(testKey);

        // Make sure the metadata no longer exists (fetch returns null)
        assertNull(api.getDocumentMetadata(testKey));
    }

    @Test
    public void testActivityLog() throws IOException, InterruptedException, RiakException {
        IRiakClient client = TestClient.getTestClient();
        DocumentStore api = new DocumentStore(client);

        Document document = getTestDocument();

        String testKey = document.getId();
        byte[] testValue = document.getContents();

        // ensure no entries exist in ActivityLog with this key
        assertNull(api.getActivityLog(testKey));

        // Store the document contents in Riak
        api.insertDocument(document);

        // ensure ActivityLog exists and has been set to Status=COMPLETED
        ActivityLog activityLog = api.getActivityLog(testKey);
        assertEquals(ActivityLog.ActivityLogStatus.COMPLETED, activityLog.getStatus());

        // Delete/clean up
        api.deleteDocumentContents(testKey);

        // Make sure the document no longer exists (fetch returns null)
        assertNull(api.getDocumentContents(testKey));

    }

    @Test
    public void testStoreOrgIndexList() throws RiakException, IOException, InterruptedException {
        IRiakClient client = getTestClient();
        DocumentStore api = new DocumentStore(client);
        Document document = getTestDocument();

        DocumentMetadata metadata = document.getMetadata();
        String testOrgId = metadata.getOrganizationId();
        // System.out.println(testOrgId);

        api.addToOrgIndex(document);

        // Fetch the new list, make sure the key is there
        DocumentList newList = api.getOrgIndex(testOrgId);
        assertTrue(newList.contains(document.getId()));

        // Now remove the key from the index
        api.removeFromOrgIndex(document);

        // Fetch new list, make sure key is removed
        newList = api.getOrgIndex(testOrgId);
        assertTrue(!newList.contains(document.getId()));
    }
}
