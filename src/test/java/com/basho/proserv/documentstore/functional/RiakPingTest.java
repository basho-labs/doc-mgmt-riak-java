package com.basho.proserv.documentstore.functional;

import java.io.IOException;

import org.junit.Test;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.IRiakClient;
import com.basho.proserv.documentstore.functional.TestClient;

public class RiakPingTest
{
    @Test
    public void testRiakPing() throws IOException, InterruptedException, RiakException {
        // This test creates a PB RiakClient and issues a ping() request
        IRiakClient client = TestClient.getTestClient();
        client.ping();  // Throws a RiakException if it can't connect
    }
}
