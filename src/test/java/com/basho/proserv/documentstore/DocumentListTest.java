package com.basho.proserv.documentstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DocumentListTest {
    public DocumentList getTestDocumentList() {
        String[] keyList = {"doc123", "doc345", "doc678"};
        return new DocumentList(keyList);
    }

    @Test
    public void testDocumentListEquals() {
        DocumentList list1 = getTestDocumentList();
        DocumentList list2 = getTestDocumentList();
        assertEquals(list1, list2);
    }

    @Test
    public void testDocumentListToJson() throws JsonProcessingException {
        DocumentList keys = getTestDocumentList();
        keys.key = "testKey";
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(keys);
        String expected = "[\"doc678\",\"doc345\",\"doc123\"]";
        assertEquals(json, expected);
    }
}
