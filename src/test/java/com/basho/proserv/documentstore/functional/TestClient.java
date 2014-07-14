package com.basho.proserv.documentstore.functional;

import java.io.IOException;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.pbc.PBClientConfig;

public class TestClient
{
    public static IRiakClient getTestClient() throws IOException, InterruptedException, RiakException {
        RiakServerHelper.ensureRunning();
        PBClientConfig pbTestConfig = PBClientConfig.Builder
                .from(PBClientConfig.defaults())
                .withHost(RiakServerHelper.getHost())
                .withPort(RiakServerHelper.getPort())
                .build();
        IRiakClient client = RiakFactory.newClient(pbTestConfig);
        return client;
    }
}
