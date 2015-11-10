package com.meltwater.puppy.endtoend;

import com.google.gson.Gson;
import com.insightfullogic.lambdabehave.JunitSuiteRunner;
import com.meltwater.puppy.Main;
import com.meltwater.puppy.rest.RestRequestBuilder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.ImmutableMap.of;
import static com.insightfullogic.lambdabehave.Suite.describe;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_BINDING_EXCHANGE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_BINDING_QUEUE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_EXCHANGES_SINGLE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_PERMISSIONS_SINGLE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_QUEUES_SINGLE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_USERS_SINGLE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_VHOSTS_SINGLE;

@RunWith(JunitSuiteRunner.class)
public class RabbitPuppyEndToEndTest {
    {
        final Properties properties = new Properties() {{
            try {
                load(ClassLoader.getSystemResourceAsStream("test.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }};

        final Gson gson = new Gson();

        final String brokerAddress = properties.getProperty("rabbit.broker.address");
        final String brokerUser = properties.getProperty("rabbit.broker.user");
        final String brokerPass = properties.getProperty("rabbit.broker.pass");

        final String VHOST = "endtoend";

        final RestRequestBuilder req = new RestRequestBuilder()
                .withHost(brokerAddress)
                .withAuthentication(brokerUser, brokerPass)
                .withHeader("content-type", "application/json");

        String configPath = ClassLoader.getSystemResource("endtoend.yaml").getPath();

        describe("a rabbit-puppy with configuration and external rabbit", it -> {

            it.isSetupWith(() -> Main.run(new String[]{
                    "--broker", brokerAddress,
                    "--user", brokerUser,
                    "--pass", brokerPass,
                    "--config", configPath}));

            it.isConcludedWith(() -> {
                req.request(PATH_VHOSTS_SINGLE, of("vhost", VHOST)).delete();
                req.request(PATH_USERS_SINGLE, of("user", "test_dan")).delete();
            });

            it.should("create vhost", expect -> {
                Map map = gson.fromJson(getString(req, PATH_VHOSTS_SINGLE, of(
                                "vhost", VHOST)),
                        Map.class);

                expect.that(map.get("name")).is(VHOST);
            });

            it.should("creates user", expect -> {
                Map map = gson.fromJson(getString(req, PATH_USERS_SINGLE, of(
                                "user", "test_dan")),
                        Map.class);

                expect.that(map.get("tags")).is("administrator");
            });

            it.should("creates permissions", expect -> {
                Map map = gson.fromJson(getString(req, PATH_PERMISSIONS_SINGLE, of(
                                "vhost", VHOST,
                                "user", "test_dan")),
                        Map.class);

                expect
                        .that(map.get("configure")).is(".*")
                        .and(map.get("write")).is(".*")
                        .and(map.get("read")).is(".*");
            });

            it.should("creates exchange 'exchange.in@test'", expect -> {
                Map map = gson.fromJson(getString(
                                req.nextWithAuthentication("test_dan", "torrance"),
                                PATH_EXCHANGES_SINGLE,
                                of(
                                        "exchange", "exchange.in",
                                        "vhost", VHOST)),
                        Map.class);

                expect.that(map.get("type")).is("topic")
                        .and(map.get("durable")).is(false)
                        .and(map.get("auto_delete")).is(true)
                        .and(map.get("internal")).is(true)
                        .and(((Map) map.get("arguments")).size()).is(1)
                        .and(((Map) map.get("arguments")).get("hash-header")).is("abc");
            });

            it.should("creates exchange 'exchange.out@test'", expect -> {
                Map map = gson.fromJson(getString(
                                req.nextWithAuthentication("test_dan", "torrance"),
                                PATH_EXCHANGES_SINGLE,
                                of(
                                        "exchange", "exchange.out",
                                        "vhost", VHOST)),
                        Map.class);

                expect.that(map.get("type")).is("direct")
                        .and(map.get("durable")).is(true)
                        .and(map.get("auto_delete")).is(false)
                        .and(map.get("internal")).is(false)
                        .and(((Map) map.get("arguments")).size()).is(0);
            });

            it.should("creates queue", expect -> {
                Map map = gson.fromJson(getString(
                                req.nextWithAuthentication("test_dan", "torrance"),
                                PATH_QUEUES_SINGLE,
                                of(
                                        "queue", "queue-test",
                                        "vhost", VHOST)),
                        Map.class);

                expect.that(map.get("durable")).is(false)
                        .and(map.get("auto_delete")).is(true)
                        .and(((Map) map.get("arguments")).size()).is(1)
                        .and(((Map) map.get("arguments")).get("x-message-ttl")).isIn(123, 123.0);
            });

            it.should("creates binding to queue", expect -> {
                Map map = (Map) gson.fromJson(getString(
                                req.nextWithAuthentication("test_dan", "torrance"),
                                PATH_BINDING_QUEUE,
                                of(
                                        "exchange", "exchange.in",
                                        "to", "queue-test",
                                        "vhost", VHOST)),
                        List.class).get(0);

                expect.that(map.get("routing_key")).is("route-queue")
                        .and(((Map) map.get("arguments")).size()).is(1)
                        .and(((Map) map.get("arguments")).get("foo")).is("bar");
            });

            it.should("creates binding to exchange", expect -> {
                Map map = (Map) gson.fromJson(getString(
                                req.nextWithAuthentication("test_dan", "torrance"),
                                PATH_BINDING_EXCHANGE,
                                of(
                                        "exchange", "exchange.in",
                                        "to", "exchange.out",
                                        "vhost", VHOST)),
                        List.class).get(0);

                expect.that(map.get("routing_key")).is("route-exchange")
                        .and(((Map) map.get("arguments")).size()).is(1)
                        .and(((Map) map.get("arguments")).get("cat")).is("dog");
            });
        });
    }

    private String getString(RestRequestBuilder requestBuilder, String path, Map<String, String> params) {
        return requestBuilder
                .request(path, params)
                .get()
                .readEntity(String.class);
    }
}
