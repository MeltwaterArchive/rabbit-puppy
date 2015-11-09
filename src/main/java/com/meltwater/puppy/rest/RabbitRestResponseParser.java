package com.meltwater.puppy.rest;

import com.google.gson.Gson;
import com.meltwater.puppy.config.BindingData;
import com.meltwater.puppy.config.ExchangeData;
import com.meltwater.puppy.config.PermissionsData;
import com.meltwater.puppy.config.QueueData;
import com.meltwater.puppy.config.UserData;
import com.meltwater.puppy.config.VHostData;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RabbitRestResponseParser {

    private final Gson gson = new Gson();

    public Map<String, VHostData> vhosts(Response response) throws RestClientException {
        try {
            List list = gson.fromJson(response.readEntity(String.class), List.class);
            Map<String, VHostData> vhosts = new HashMap<>();
            for (int i = 0; i < list.size(); i++) {
                Map map = (Map) list.get(i);
                String name = (String) map.get("name");
                VHostData vHostData = new VHostData((boolean) map.get("tracing"));
                vhosts.put(name, vHostData);
            }
            return vhosts;
        } catch (Exception e) {
            throw new RestClientException("Error parsing vhosts response", e);
        }
    }

    public Map<String, UserData> users(Response response) throws RestClientException {
        try {
            List list = gson.fromJson(response.readEntity(String.class), List.class);
            Map<String, UserData> users = new HashMap<>();
            for (int i = 0; i < list.size(); i++) {
                Map map = (Map) list.get(i);
                String name = (String) map.get("name");
                boolean admin = ((String) map.get("tags")).contains("administrator");
                UserData userData = new UserData(null, admin);
                users.put(name, userData);
            }
            return users;
        } catch (Exception e) {
            throw new RestClientException("Error parsing users response", e);
        }
    }

    public Map<String, PermissionsData> permissions(Response response) throws RestClientException {
        try {
            List list = gson.fromJson(response.readEntity(String.class), List.class);
            Map<String, PermissionsData> permissions = new HashMap<>();
            for (int i = 0; i < list.size(); i++) {
                Map map = (Map) list.get(i);
                String user = (String) map.get("user");
                String vhost = (String) map.get("vhost");
                PermissionsData permissionsData = new PermissionsData(
                        (String) map.get("configure"),
                        (String) map.get("write"),
                        (String) map.get("read"));
                permissions.put(user + "@" + vhost, permissionsData);
            }
            return permissions;
        } catch (Exception e) {
            throw new RestClientException("Error parsing permissions response", e);
        }
    }

    public Optional<ExchangeData> exchange(Optional<Response> response) throws RestClientException {
        if (!response.isPresent()) {
            return Optional.empty();
        }
        try {
            Map map = gson.fromJson(response.get().readEntity(String.class), Map.class);
            return Optional.of(new ExchangeData(
                    (String) map.get("type"),
                    (boolean) map.get("durable"),
                    (boolean) map.get("auto_delete"),
                    (boolean) map.get("internal"),
                    (Map) map.get("arguments")));
        } catch (Exception e) {
            throw new RestClientException("Error parsing exchanges response", e);
        }
    }

    public Optional<QueueData> queue(Optional<Response> response) throws RestClientException {
        if (!response.isPresent()) {
            return Optional.empty();
        }
        try {
            Map map = gson.fromJson(response.get().readEntity(String.class), Map.class);
            return Optional.of(new QueueData(
                    (boolean) map.get("durable"),
                    (boolean) map.get("auto_delete"),
                    (Map) map.get("arguments")));
        } catch (Exception e) {
            throw new RestClientException("Error parsing exchanges response", e);
        }
    }

    public Map<String, List<BindingData>> bindings(Response response) throws RestClientException {
        try {
            List list = gson.fromJson(response.readEntity(String.class), List.class);
            Map<String, List<BindingData>> bindings = new HashMap<>();
            for (int i = 0; i < list.size(); i++) {
                Map map = (Map) list.get(i);
                String exchange = (String) map.get("source");
                if (!bindings.containsKey(exchange)) {
                    bindings.put(exchange, new ArrayList<>());
                }
                BindingData bindingData = new BindingData(
                        (String) map.get("destination"),
                        (String) map.get("destination_type"),
                        (String) map.get("routing_key"),
                        (Map) map.get("arguments"));
                bindings.get(exchange).add(bindingData);
            }
            return bindings;
        } catch (Exception e) {
            throw new RestClientException("Error parsing bindings response", e);
        }
    }
}
