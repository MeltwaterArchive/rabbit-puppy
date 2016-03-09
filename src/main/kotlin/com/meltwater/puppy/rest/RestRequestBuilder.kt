package com.meltwater.puppy.rest

import com.google.common.net.UrlEscapers
import org.glassfish.jersey.client.JerseyClientBuilder
import org.glassfish.jersey.client.JerseyInvocation
import org.glassfish.jersey.client.JerseyWebTarget
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature
import org.slf4j.LoggerFactory
import java.util.*
import java.util.Optional.empty

class RestRequestBuilder(var host: String, var auth: Pair<String, String>) {

    private val log = LoggerFactory.getLogger(RestRequestBuilder::class.java)

    private val client = JerseyClientBuilder.createClient()
    private val escaper = UrlEscapers.urlPathSegmentEscaper()

    private var authNext: Optional<Pair<String, String>> = empty()
    private var headers = HashMap<String, String>()

    fun nextWithAuthentication(authUser: String, authPass: String): RestRequestBuilder {
        authNext = Optional.of(Pair(authUser, authPass))
        return this
    }

    fun withHeader(header: String, value: String): RestRequestBuilder {
        headers.put(header, value)
        return this
    }

    fun request(path: String): JerseyInvocation.Builder {
        val url = hostAnd(path)
        log.trace("Building request to $url")
        return addProperties(client.target(url)).request()
    }

    fun request(path: String, routeParams: Map<String, String>): JerseyInvocation.Builder {
        var p: String = path
        for (entry in routeParams.entries) {
            p = p.replace("{${entry.key}}", escaper.escape(entry.value))
        }
        return request(p)
    }

    fun getAuthUser(): String = auth.first
    fun getAuthPass(): String = auth.second

    private fun addProperties(target: JerseyWebTarget): JerseyWebTarget {
        for (entry in headers.entries) {
            target.property(entry.key, entry.value)
        }
        if (authNext.isPresent) {
            target.register(HttpAuthenticationFeature.basic(authNext.get().first, authNext.get().second))
            authNext = empty()
        } else {
            target.register(HttpAuthenticationFeature.basic(auth.first, auth.second))
        }
        return target
    }

    private fun hostAnd(path: String): String = host + path
}
