package com.github.ram042.shortener.analytics;

import io.javalin.Javalin;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua_parser.Parser;

import java.util.Optional;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Javalin.create(config -> {
                    config.showJavalinBanner = false;
                })
                .post("/", ctx -> {
                    LOGGER.info(ctx.body());
                    var key = new JSONObject(ctx.body())
                            .getJSONArray("messages").getJSONObject(0)
                            .getJSONObject("details").getJSONObject("message")
                            .getString("body");
                    var rawUserAgent = new JSONObject(ctx.body())
                            .getJSONArray("messages").getJSONObject(0)
                            .getJSONObject("details").getJSONObject("message")
                            .getJSONObject("message_attributes")
                            .getJSONObject("useragent").getString("string_value");
                    var ua = new Parser().parse(rawUserAgent);
                    LOGGER.info("Key '{}' requested by {} on {}", key, ua.userAgent.family, ua.os.family);
                })
                .start(Integer.parseInt(
                        Optional.ofNullable(System.getenv("PORT")).orElse("8080")
                ));
    }
}