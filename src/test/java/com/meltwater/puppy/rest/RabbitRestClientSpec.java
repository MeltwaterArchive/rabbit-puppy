package com.meltwater.puppy.rest;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.insightfullogic.lambdabehave.JunitSuiteRunner;
import com.meltwater.puppy.config.ExchangeData;
import com.meltwater.puppy.config.PermissionsData;
import com.meltwater.puppy.config.VHostData;
import org.junit.runner.RunWith;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.collect.ImmutableMap.of;
import static com.insightfullogic.lambdabehave.Suite.describe;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_EXCHANGES_SINGLE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_PERMISSIONS_SINGLE;
import static com.meltwater.puppy.rest.RabbitRestClient.PATH_VHOSTS_SINGLE;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@RunWith(JunitSuiteRunner.class)
public class RabbitRestClientSpec {
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

        final RestRequestBuilder req = new RestRequestBuilder()
                .withHost(brokerAddress)
                .withAuthentication(brokerUser, brokerPass)
                .withHeader("content-type", "application/json");

        describe("a RabbitMQ REST client with valid auth credentials", it -> {

            final RabbitRestClient rabbitRestClient = new RabbitRestClient(brokerAddress, brokerUser, brokerPass);

            it.isSetupWith(() -> {
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test"))
                        .put(Entity.entity(gson.toJson(new VHostData()), MediaType.APPLICATION_JSON_TYPE));
                req.request(PATH_PERMISSIONS_SINGLE, of("vhost", "test", "user", "guest"))
                        .put(Entity.entity(gson.toJson(new PermissionsData()), MediaType.APPLICATION_JSON_TYPE));
                req.request(PATH_EXCHANGES_SINGLE, of("vhost", "test", "exchange", "test.ex"))
                        .put(Entity.entity(gson.toJson(new ExchangeData()), MediaType.APPLICATION_JSON_TYPE));
            });

            it.isConcludedWith(() -> {
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test")).delete();
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test1")).delete();
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test2")).delete();
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test/test")).delete();
            });

            it
                    .uses("test1", new VHostData(false))
                    .and("test2", new VHostData(true))
                    .toShow("creates vhost: %s", (expect, vhost, data) -> {
                        rabbitRestClient.createVirtualHost(vhost, data);

                        Map map = gson.fromJson(getString(req, PATH_VHOSTS_SINGLE, of("vhost", vhost)), Map.class);
                        expect.that(map.get("tracing")).is(data.isTracing());
                    });

            it.should("gets existing vhosts", expect -> {
                Map<String, VHostData> virtualHosts = rabbitRestClient.getVirtualHosts();
                expect
                        .that(virtualHosts.keySet())
                        .hasSize(greaterThanOrEqualTo(1))
                        .hasItem("/");

                expect
                        .that(virtualHosts.get("/"))
                        .isNotNull()
                        .instanceOf(VHostData.class);
            });

            it
                    .uses("ex1", "test", exchangeOfType("fanout"))
                    .and("ex2", "test", exchangeOfType("direct"))
                    .and("ex3", "test", exchangeOfType("headers"))
                    .and("ex4", "test", new ExchangeData("topic", false, true, true, of("foo", "bar")))
                    .toShow("creates exchange: %s", (expect, exchange, vhost, data) -> {
                        rabbitRestClient.createExchange(vhost, exchange, data, brokerUser, brokerUser);

                        ExchangeData response = gson.fromJson(getString(req, PATH_EXCHANGES_SINGLE, of("vhost", vhost, "exchange", exchange)), ExchangeData.class);
                        expect.that(response).is(data);
                    });

            it.should("gets existing exchange", expect -> {
                Optional<ExchangeData> exchange = rabbitRestClient.getExchange("/", "amq.direct", brokerUser, brokerPass);
                expect
                        .that(exchange.isPresent())
                        .is(true)
                        .and(exchange.get())
                        .isNotNull();
            });

            it.should("does not get non-existing exchange", expect -> {
                Optional<ExchangeData> exchange = rabbitRestClient.getExchange("/", "amq.NOPE", brokerUser, brokerPass);
                expect
                        .that(exchange.isPresent())
                        .is(false);
            });

            // TODO test get/create users, permissions, queues, bindings

        });
    }

    private static ExchangeData exchangeOfType(String type) {
        ExchangeData exchangeData = new ExchangeData();
        exchangeData.setType(type);
        return exchangeData;
    }

    private String getString(RestRequestBuilder requestBuilder, String path, Map<String, String> params) {
        return requestBuilder
                .request(path, params)
                .get()
                .readEntity(String.class);
    }
}