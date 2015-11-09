package com.meltwater.puppy.rest;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.JerseyInvocation;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import java.util.HashMap;
import java.util.Map;

public class RestRequestBuilder {

    private String host = null;
    private String authUser = null;
    private String authPass = null;
    private String authUserNext = null;
    private String authPassNext = null;
    private Map<String, String> headers = new HashMap<>();

    private final JerseyClient client = JerseyClientBuilder.createClient();
    private final Escaper escaper = UrlEscapers.urlPathSegmentEscaper();

    public RestRequestBuilder() {
    }

    public RestRequestBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public RestRequestBuilder withAuthentication(String authUser, String authPass) {
        this.authUser = authUser;
        this.authPass = authPass;
        return this;
    }

    public RestRequestBuilder nextWithAuthentication(String authUser, String authPass) {
        this.authUserNext = authUser;
        this.authPassNext = authPass;
        return this;
    }

    public RestRequestBuilder withHeader(String header, String value) {
        headers.put(header, value);
        return this;
    }

    public JerseyInvocation.Builder request(String path) {
        return addProperties(client.target(hostAnd(path))).request();
    }

    public JerseyInvocation.Builder request(String path, Map<String, String> routeParams) {
        for (Map.Entry<String, String> entry : routeParams.entrySet()) {
            path = path.replace("{"+entry.getKey()+"}", escaper.escape(entry.getValue()));
        }
        return request(path);
    }

    public String getHost() {
        return host;
    }

    public String getAuthUser() {
        return authUser;
    }

    public String getAuthPass() {
        return authPass;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    private JerseyWebTarget addProperties(JerseyWebTarget target) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            target = target.property(entry.getKey(), entry.getValue());
        }
        if (authUserNext != null && authPassNext != null) {
            target = target.register(HttpAuthenticationFeature.basic(authUserNext, authPassNext));
            authUserNext = null;
            authPassNext = null;
        } else if (authUser != null && authPass != null) {
            target = target.register(HttpAuthenticationFeature.basic(authUser, authPass));
        }
        return target;
    }

    private String hostAnd(String path) {
        return host == null ? path : host + path;
    }
}
