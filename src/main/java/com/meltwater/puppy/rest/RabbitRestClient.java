package com.meltwater.puppy.rest;

import com.google.gson.Gson;
import com.meltwater.puppy.config.BindingData;
import com.meltwater.puppy.config.ExchangeData;
import com.meltwater.puppy.config.PermissionsData;
import com.meltwater.puppy.config.QueueData;
import com.meltwater.puppy.config.UserData;
import com.meltwater.puppy.config.VHostData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.format;
import static javax.ws.rs.client.Entity.entity;

public class RabbitRestClient {

    private static final Logger log = LoggerFactory.getLogger(RabbitRestClient.class);

    public static final String PATH_OVERVIEW = "api/overview";
    public static final String PATH_VHOSTS = "api/vhosts";
    public static final String PATH_VHOSTS_SINGLE = "api/vhosts/{vhost}";
    public static final String PATH_USERS = "api/users";
    public static final String PATH_USERS_SINGLE = "api/users/{user}";
    public static final String PATH_PERMISSIONS = "api/permissions";
    public static final String PATH_PERMISSIONS_SINGLE = "api/permissions/{vhost}/{user}";
    public static final String PATH_EXCHANGES_SINGLE = "api/exchanges/{vhost}/{exchange}";
    public static final String PATH_QUEUES_SINGLE = "api/queues/{vhost}/{queue}";
    public static final String PATH_BINDINGS_VHOST = "api/bindings/{vhost}";
    public static final String PATH_BINDING_QUEUE = "api/bindings/{vhost}/e/{exchange}/q/{to}";
    public static final String PATH_BINDING_EXCHANGE = "api/bindings/{vhost}/e/{exchange}/e/{to}";

    private final RestRequestBuilder requestBuilder;
    private final RabbitRestResponseParser parser = new RabbitRestResponseParser();
    private final Gson gson = new Gson();

    public RabbitRestClient(String brokerAddress, String brokerUsername, String brokerPassword) {
        this.requestBuilder = new RestRequestBuilder()
                .withHost(brokerAddress)
                .withAuthentication(brokerUsername, brokerPassword)
                .withHeader("content-type", "application/json");
    }

    public boolean ping() {
        try {
            Response response = requestBuilder.request(PATH_OVERVIEW).get();
            return response.getStatus() == Status.OK.getStatusCode();
        } catch (Exception e) {
            return false;
        }
    }

    public void createVirtualHost(String virtualHost, VHostData vHostData) throws RestClientException {
        expect(requestBuilder
                        .request(PATH_VHOSTS_SINGLE, of("vhost", virtualHost))
                        .put(entity(gson.toJson(vHostData), MediaType.APPLICATION_JSON_TYPE)),
                Status.NO_CONTENT.getStatusCode());
    }

    public Map<String, VHostData> getVirtualHosts() throws RestClientException {
        return parser.vhosts(
                expect(requestBuilder.request(PATH_VHOSTS).get(),
                        Status.OK.getStatusCode()));
    }

    public void createUser(String user, UserData userData) throws RestClientException {
        require("User", user, "password", userData.getPassword());

        expect(requestBuilder
                        .request(PATH_USERS_SINGLE, of("user", user))
                        .put(entity(gson.toJson(of(
                                "password", userData.getPassword(),
                                "tags", userData.getAdmin() ? "administrator" : ""
                        )), MediaType.APPLICATION_JSON_TYPE)),
                Status.NO_CONTENT.getStatusCode());
    }

    public Map<String, UserData> getUsers() throws RestClientException {
        return parser.users(
                expect(requestBuilder.request(PATH_USERS).get(),
                        Status.OK.getStatusCode()));
    }

    public void createPermissions(String user, String vhost, PermissionsData permissionsData) throws RestClientException {
        require("Permissions", format("%s@%s", user, vhost), "configure", permissionsData.getConfigure());
        require("Permissions", format("%s@%s", user, vhost), "write", permissionsData.getWrite());
        require("Permissions", format("%s@%s", user, vhost), "read", permissionsData.getRead());

        expect(requestBuilder
                        .request(PATH_PERMISSIONS_SINGLE, of(
                                "vhost", vhost,
                                "user", user))
                        .put(entity(gson.toJson(permissionsData), MediaType.APPLICATION_JSON_TYPE)),
                Status.NO_CONTENT.getStatusCode());
    }

    public Map<String, PermissionsData> getPermissions() throws RestClientException {
        return parser.permissions(
                expect(requestBuilder.request(PATH_PERMISSIONS).get(),
                        Status.OK.getStatusCode()));
    }

    public void createExchange(String vhost,
                               String exchange,
                               ExchangeData exchangeData,
                               String user,
                               String pass) throws RestClientException {
        require("Exchange", exchange + "@" + vhost, "type", exchangeData.getType());

        expect(requestBuilder
                        .nextWithAuthentication(user, pass)
                        .request(PATH_EXCHANGES_SINGLE, of(
                                "vhost", vhost,
                                "exchange", exchange))
                        .put(entity(gson.toJson(exchangeData), MediaType.APPLICATION_JSON_TYPE)),
                Status.NO_CONTENT.getStatusCode());
    }

    public Optional<ExchangeData> getExchange(String vhost,
                                              String exchange,
                                              String user,
                                              String pass) throws RestClientException {
        return parser.exchange(
                expectOrEmpty(requestBuilder
                                .nextWithAuthentication(user, pass)
                                .request(PATH_EXCHANGES_SINGLE, of(
                                        "vhost", vhost,
                                        "exchange", exchange))
                                .get(),
                        Status.OK.getStatusCode(),
                        Status.NOT_FOUND.getStatusCode()));
    }

    public void createQueue(String vhost, String queue,
                            QueueData queueData,
                            String user,
                            String pass) throws RestClientException {
        expect(requestBuilder
                        .nextWithAuthentication(user, pass)
                        .request(PATH_QUEUES_SINGLE, of(
                                "vhost", vhost,
                                "queue", queue))
                        .put(entity(gson.toJson(queueData), MediaType.APPLICATION_JSON_TYPE)),
                Status.NO_CONTENT.getStatusCode());
    }

    public Optional<QueueData> getQueue(String vhost,
                                        String queue,
                                        String user,
                                        String pass) throws RestClientException {
        return parser.queue(
                expectOrEmpty(requestBuilder
                                .nextWithAuthentication(user, pass)
                                .request(PATH_QUEUES_SINGLE, of(
                                        "vhost", vhost,
                                        "queue", queue))
                                .get(),
                        Status.OK.getStatusCode(),
                        Status.NOT_FOUND.getStatusCode()));
    }

    public void createBinding(String vhost,
                              String exchange,
                              BindingData bindingData,
                              String user,
                              String pass) throws RestClientException {
        require("Binding", format("%s@%s", user, vhost), "destination", bindingData.getDestination());
        require("Binding", format("%s@%s", user, vhost), "destination_type", bindingData.getDestination_type());
        require("Binding", format("%s@%s", user, vhost), "routing_key", bindingData.getRouting_key());

        if (bindingData.getDestination_type().equals("queue")) {
            expect(requestBuilder
                            .nextWithAuthentication(user, pass)
                            .request(PATH_BINDING_QUEUE, of(
                                    "vhost", vhost,
                                    "exchange", exchange,
                                    "to", bindingData.getDestination()))
                            .post(entity(gson.toJson(of(
                                            "routing_key", bindingData.getRouting_key(),
                                            "arguments", bindingData.getArguments())),
                                    MediaType.APPLICATION_JSON_TYPE)),
                    Status.CREATED.getStatusCode());
        } else if (bindingData.getDestination_type().equals("exchange")) {
            expect(requestBuilder
                            .nextWithAuthentication(user, pass)
                            .request(PATH_BINDING_EXCHANGE, of(
                                    "vhost", vhost,
                                    "exchange", exchange,
                                    "to", bindingData.getDestination()))
                            .post(entity(gson.toJson(of(
                                            "routing_key", bindingData.getRouting_key(),
                                            "arguments", bindingData.getArguments())),
                                    MediaType.APPLICATION_JSON_TYPE)),
                    Status.CREATED.getStatusCode());
        } else {
            throw new RestClientException(format("Invalid binding destination: %s", bindingData.getDestination()));
        }
    }

    public Map<String, List<BindingData>> getBindings(String vhost,
                                                      String user,
                                                      String pass) throws RestClientException {
        return parser.bindings(
                expect(requestBuilder
                                .nextWithAuthentication(user, pass)
                                .request(PATH_BINDINGS_VHOST, of(
                                        "vhost", vhost))
                                .get(),
                        Status.OK.getStatusCode()));
    }

    public String getUsername() {
        return requestBuilder.getAuthUser();
    }

    public String getPassword() {
        return requestBuilder.getAuthUser();
    }

    private <D> void require(String type, String name, String property, D value) throws RestClientException {
        if (value == null) {
            throw new RestClientException(format("%s %s missing required field: %s", type, name, property));
        }
    }

    private static Response expect(Response response, int statusExpected) throws RestClientException {
        if (response.getStatus() != statusExpected) {
            String error = String.format("Response with HTTP status %d %s, expected status code %d",
                    response.getStatus(), response.getStatusInfo().getReasonPhrase(), statusExpected);
            log.error(error);
            throw new RestClientException(error);
        }
        return response;
    }

    private static Optional<Response> expectOrEmpty(Response response,
                                                   int statusExpected,
                                                   int statusEmpty) throws RestClientException {
        if (response.getStatus() == statusExpected) {
            return Optional.of(response);
        } else if (response.getStatus() == statusEmpty) {
            return Optional.empty();
        } else {
            String error = String.format("Response with HTTP status %d %s, expected status code %d or %s",
                    response.getStatus(), response.getStatusInfo().getReasonPhrase(), statusExpected, statusEmpty);
            log.error(error);
            throw new RestClientException(error);
        }
    }
}
