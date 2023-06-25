package com.github.ram042.shortener.admin;

import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    private final SessionRetryContext db;
    private final String auth;

    public Main(SessionRetryContext db, @Value("${AUTH}") String auth) {
        this.db = db;
        this.auth = auth;
    }

    public record GetUrlInfoParams(
            String auth,
            String key
    ) {

    }

    public record GetUrlInfoResult(
            String key,
            String target
    ) {

    }

    @GetMapping(
            consumes = "application/json",
            produces = "application/json"
    )
    public GetUrlInfoResult getUrl(@RequestBody GetUrlInfoParams params) throws ExecutionException, InterruptedException {
        if (params == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "empty");
        }
        if (params.auth == null || params.key == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "empty params");
        }
        if (!auth.equals(params.auth)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        @Language("SQL")
        String getKeyQuery = """
                declare $key as utf8;
                select * 
                from urls
                where key = $key;
                """;
        var result = db.supplyResult(session -> session.executeDataQuery(
                getKeyQuery, TxControl.serializableRw(), Params.of("$key", PrimitiveValue.newText(params.key))
        )).get().getValue().getResultSet(0);

        if (result.next()) {
            return new GetUrlInfoResult(
                    result.getColumn("key").getText(),
                    result.getColumn("target").getText()
            );
        } else {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
    }

    public record PutUrlParams(
            String auth,
            String key,
            String target
    ) {

    }

    @GetMapping(
            consumes = "application/json",
            produces = "application/json"
    )
    public void putUrl(@RequestBody PutUrlParams params) throws ExecutionException, InterruptedException {
        if (params == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "empty");
        }
        if (params.auth == null || params.key == null || params.target == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "empty params");
        }
        if (!auth.equals(params.auth)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        @Language("SQL")
        String getKeyQuery = """
                declare $key as utf8;
                declare $target as utf8;
                insert into urls (key, target)
                values ($key, $target);
                """;
        var status = db.supplyResult(session -> session.executeDataQuery(
                getKeyQuery, TxControl.serializableRw(), Params.of(
                        "$key", PrimitiveValue.newText(params.key),
                        "$target", PrimitiveValue.newText(params.target)
                )
        )).get().getStatus();

        if (!status.isSuccess()) {
            LOGGER.info("Cannot add url: {}", status);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


}