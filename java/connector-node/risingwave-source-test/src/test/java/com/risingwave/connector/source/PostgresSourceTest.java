// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.source;

import static org.junit.Assert.*;

import com.risingwave.connector.ConnectorServiceImpl;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import javax.sql.DataSource;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresSourceTest {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceTest.class.getName());

    public static Server connectorServer =
            ServerBuilder.forPort(SourceTestClient.DEFAULT_PORT)
                    .addService(new ConnectorServiceImpl())
                    .build();

    public static SourceTestClient testClient =
            new SourceTestClient(
                    Grpc.newChannelBuilder(
                                    "localhost:" + SourceTestClient.DEFAULT_PORT,
                                    InsecureChannelCredentials.create())
                            .build());

    private static DataSource pgDataSource;

    @BeforeClass
    public static void init() {}

    @AfterClass
    public static void cleanup() {
        connectorServer.shutdown();
    }

    // create a TPC-H orders table in postgres
    // insert 10,000 rows into orders
    // check if the number of changes debezium captures is 10,000
    @Test
    public void testLines() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        // var jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.POSTGRES, "localhost", "8432",
        // "mydb");
        // var user = "myuser";
        // var password = "123456";
        Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                testClient.getEventStreamStart(
                        ConnectorServiceProto.SourceType.POSTGRES, "mydb", "t4");
        Callable<Integer> countTask =
                () -> {
                    int count = 0;
                    while (eventStream.hasNext()) {
                        List<ConnectorServiceProto.CdcMessage> messages =
                                eventStream.next().getEventsList();
                        for (ConnectorServiceProto.CdcMessage msg : messages) {
                            if (!msg.getPayload().isBlank()) {
                                count++;
                            }
                        }
                        if (count >= 10000) {
                            return count;
                        }
                    }
                    return count;
                };
        Future<Integer> countResult = executorService.submit(countTask);
        int count = countResult.get();
        LOG.info("number of cdc messages received: {}", count);
    }

    // test whether validation catches permission errors
    // generates test cases for the risingwave debezium parser
}
