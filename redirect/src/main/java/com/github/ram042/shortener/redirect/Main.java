package com.github.ram042.shortener.redirect;

import io.javalin.Javalin;
import org.intellij.lang.annotations.Language;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Main {
    public static void main(String[] args) {
        @Language("SQL")
        String getKeyQuery = """
                declare $key as utf8;
                select * 
                from urls
                where key = $key;
                """;
        var dbFuture = getDatabase();
        var sqsFuture = getSqsClient();
        var app = Javalin.create(config -> {
                    config.showJavalinBanner = false;
                })
                .get("/{key}", ctx -> {
                    var key = ctx.pathParam("key");
                    var db = dbFuture.get();
                    var result = db.supplyResult(session -> session.executeDataQuery(
                            getKeyQuery, TxControl.serializableRw(), Params.of("$key", PrimitiveValue.newText(key))
                    )).get().getValue().getResultSet(0);
                    if (result.next()) {
                        ctx.redirect(result.getColumn("target").getText());

                        sqsFuture.thenAccept(sqsClient -> {
                            sqsClient.sendMessage(builder -> builder
                                    .messageBody(key)
                                    .queueUrl(System.getenv("SQS_URL"))
                                    .messageAttributes(Map.of(
                                            "useragent", MessageAttributeValue.builder().dataType("String").stringValue(ctx.userAgent()).build()
                                    )));
                        });
                        return;
                    } else {
                        ctx.result("Key not found");
                        ctx.status(404);
                    }
                })
                .start(Integer.parseInt(
                        Optional.ofNullable(System.getenv("PORT")).orElse("8080")
                ));
    }

    public static CompletableFuture<SessionRetryContext> getDatabase() {
        return CompletableFuture.supplyAsync(() -> {
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
            GrpcTransport transport = GrpcTransport.forConnectionString(System.getenv("APP_DB_ENDPOINT"))
                    .withAuthProvider(dbAuthProvider)
                    .build();

            var client = TableClient.newClient(transport)
                    .sessionPoolSize(1, 5)
                    .build();
            return SessionRetryContext.create(client).build();
        });
    }

    public static CompletableFuture<SqsClient> getSqsClient() {
        return CompletableFuture.supplyAsync(() -> {
            var id = System.getenv("SQS_TOKEN_ID");
            var secret = System.getenv("SQS_TOKEN_SECRET");
            var url = System.getenv("SQS_URL");
            if (id == null || secret == null) {
                throw new IllegalArgumentException("SQS token not set");
            }
            return SqsClient.builder()
                    .region(Region.of("ru-central1"))
                    .endpointOverride(URI.create("https://message-queue.api.cloud.yandex.net/"))
                    .endpointProvider(endpointParams -> CompletableFuture.completedFuture(Endpoint.builder()
                            .url(URI.create("https://message-queue.api.cloud.yandex.net/"))
                            .build()))
                    .credentialsProvider(() -> AwsBasicCredentials.create(id, secret))
                    .build();
        });
    }
}