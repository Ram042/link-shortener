package com.github.ram042.shortener.admin;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import tech.ydb.auth.AuthRpcProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.impl.auth.GrpcAuthRpc;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class Database {

    private final SessionRetryContext db;

    public Database(@Value("${APP_DB_ENDPOINT}") String dbEndpoint) {
        var keyPath = Path.of("authorized_key.json");
        AuthRpcProvider<GrpcAuthRpc> dbAuthProvider = rpc -> {
            if (Files.exists(keyPath)) {
                try {
                    return CloudAuthHelper.getServiceAccountJsonAuthProvider(Files.readString(keyPath))
                            .createAuthIdentity(rpc);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return CloudAuthHelper.getMetadataAuthProvider().createAuthIdentity(rpc);
            }
        };
        GrpcTransport transport = GrpcTransport.forConnectionString(dbEndpoint)
                .withAuthProvider(dbAuthProvider)
                .build();
        var client = TableClient.newClient(transport)
                .sessionPoolSize(1, 20)
                .build();
        db = SessionRetryContext.create(client).build();
    }

    @Bean
    public SessionRetryContext getDb() {
        return db;
    }
}
