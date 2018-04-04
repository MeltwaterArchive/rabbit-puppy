package com.meltwater.puppy.rest

import com.google.common.collect.ImmutableMap.of
import com.google.gson.Gson
import com.meltwater.puppy.config.*
import org.slf4j.LoggerFactory
import java.util.*
import javax.ws.rs.client.Entity.entity
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status

class RestClientException : Exception {
    constructor(s: String, e: Exception) : super(s, e)
    constructor(s: String) : super(s)
}

open class RabbitRestClient(brokerAddress: String, brokerUsername: String, brokerPassword: String) {

    private val log = LoggerFactory.getLogger(RabbitRestClient::class.java)

    companion object {
        val PATH_OVERVIEW = "api/overview"
        public val PATH_VHOSTS = "api/vhosts"
        val PATH_VHOSTS_SINGLE = "api/vhosts/{vhost}"
        val PATH_USERS = "api/users"
        val PATH_USERS_SINGLE = "api/users/{user}"
        val PATH_PERMISSIONS = "api/permissions"
        val PATH_PERMISSIONS_SINGLE = "api/permissions/{vhost}/{user}"
        val PATH_EXCHANGES_SINGLE = "api/exchanges/{vhost}/{exchange}"
        val PATH_QUEUES_SINGLE = "api/queues/{vhost}/{queue}"
        val PATH_BINDINGS_VHOST = "api/bindings/{vhost}"
        val PATH_BINDING_QUEUE = "api/bindings/{vhost}/e/{exchange}/q/{to}"
        val PATH_BINDING_EXCHANGE = "api/bindings/{vhost}/e/{exchange}/e/{to}"
    }


    private val requestBuilder: RestRequestBuilder
    private val parser = RabbitRestResponseParser()
    private val gson = Gson()


    init {
        this.requestBuilder = RestRequestBuilder(brokerAddress, kotlin.Pair(brokerUsername, brokerPassword))
                .withHeader("content-type", "application/json")
    }

    open fun ping(): Boolean {
        try {
            val response = requestBuilder.request(PATH_OVERVIEW).get()
            return response.status == Status.OK.statusCode
        } catch (e: Exception) {
            return false
        }

    }

    open fun getUsername(): String = requestBuilder.getAuthUser()
    open fun getPassword(): String = requestBuilder.getAuthPass()

    @Throws(RestClientException::class)
    open fun getPermissions(): Map<String, PermissionsData> = parser.permissions(
            expect(requestBuilder.request(PATH_PERMISSIONS).get(),
                    Status.OK.statusCode))

    @Throws(RestClientException::class)
    open fun getExchange(vhost: String,
                    exchange: String,
                    user: String,
                    pass: String): Optional<ExchangeData> = parser.exchange(
            expectOrEmpty(requestBuilder.nextWithAuthentication(user, pass).request(PATH_EXCHANGES_SINGLE, of(
                    "vhost", vhost,
                    "exchange", exchange)).get(),
                    Status.OK.statusCode,
                    Status.NOT_FOUND.statusCode))

    @Throws(RestClientException::class)
    open fun getQueue(vhost: String,
                 queue: String,
                 user: String,
                 pass: String): Optional<QueueData> = parser.queue(
            expectOrEmpty(requestBuilder.nextWithAuthentication(user, pass).request(PATH_QUEUES_SINGLE, of(
                    "vhost", vhost,
                    "queue", queue)).get(),
                    Status.OK.statusCode,
                    Status.NOT_FOUND.statusCode))

    @Throws(RestClientException::class)
    open fun getBindings(vhost: String,
                    user: String,
                    pass: String): Map<String, List<BindingData>> {
        if (getVirtualHosts().contains(vhost)) {
            return parser.bindings(
                    expect(requestBuilder.nextWithAuthentication(user, pass).request(PATH_BINDINGS_VHOST, of(
                            "vhost", vhost)).get(),
                            Status.OK.statusCode))
        } else {
            return HashMap()
        }
    }

    @Throws(RestClientException::class)
    open fun getVirtualHosts(): Map<String, VHostData> = parser.vhosts(
            expect(requestBuilder.request(PATH_VHOSTS).get(),
                    Status.OK.statusCode))

    @Throws(RestClientException::class)
    open fun getUsers(): Map<String, UserData> = parser.users(
            expect(requestBuilder.request(PATH_USERS).get(),
                    Status.OK.statusCode))

    @Throws(RestClientException::class)
    open fun createVirtualHost(virtualHost: String, vHostData: VHostData) {
        expect(requestBuilder.request(PATH_VHOSTS_SINGLE, of("vhost", virtualHost)).put(entity(gson.toJson(vHostData), MediaType.APPLICATION_JSON_TYPE)),
                Status.CREATED.statusCode)
    }

    @Throws(RestClientException::class)
    open fun createUser(user: String, userData: UserData) {
        require("User", user, "password", userData.password)

        expect(requestBuilder.request(PATH_USERS_SINGLE, of("user", user)).put(entity(gson.toJson(of(
                "password", userData.password,
                "tags", if (userData.admin) "administrator" else "")), MediaType.APPLICATION_JSON_TYPE)),
                Status.CREATED.statusCode)
    }

    @Throws(RestClientException::class)
    open fun createPermissions(user: String, vhost: String, permissionsData: PermissionsData) {
        require("Permissions", "$user@$vhost", "configure", permissionsData.configure)
        require("Permissions", "$user@$vhost", "write", permissionsData.write)
        require("Permissions", "$user@$vhost", "read", permissionsData.read)

        expect(requestBuilder.request(PATH_PERMISSIONS_SINGLE, of(
                "vhost", vhost,
                "user", user)).put(entity(gson.toJson(permissionsData), MediaType.APPLICATION_JSON_TYPE)),
                Status.CREATED.statusCode)
    }

    @Throws(RestClientException::class)
    open fun createExchange(vhost: String,
                       exchange: String,
                       exchangeData: ExchangeData,
                       user: String,
                       pass: String) {
        require("Exchange", exchange + "@" + vhost, "type", exchangeData.type)

        expect(requestBuilder.nextWithAuthentication(user, pass).request(PATH_EXCHANGES_SINGLE, of(
                "vhost", vhost,
                "exchange", exchange)).put(entity(gson.toJson(exchangeData), MediaType.APPLICATION_JSON_TYPE)),
                Status.CREATED.statusCode)
    }

    @Throws(RestClientException::class)
    open fun createQueue(vhost: String, queue: String,
                    queueData: QueueData,
                    user: String,
                    pass: String) {
        expect(requestBuilder
                .nextWithAuthentication(user, pass)
                .request(PATH_QUEUES_SINGLE, of(
                        "vhost", vhost,
                        "queue", queue))
                .put(entity(gson.toJson(queueData),
                        MediaType.APPLICATION_JSON_TYPE)),
                Status.CREATED.statusCode)
    }

    @Throws(RestClientException::class)
    open fun createBinding(vhost: String,
                      exchange: String,
                      bindingData: BindingData,
                      user: String,
                      pass: String) {
        require("Binding", "$user@$vhost", "destination", bindingData.destination)
        require("Binding", "$user@$vhost", "destination_type", bindingData.destination_type)
        require("Binding", "$user@$vhost", "routing_key", bindingData.routing_key)

        if (bindingData.destination_type == DestinationType.queue) {
            expect(requestBuilder
                    .nextWithAuthentication(user, pass)
                    .request(PATH_BINDING_QUEUE, of<String, String>(
                            "vhost", vhost,
                            "exchange", exchange,
                            "to", bindingData.destination))
                    .post(entity(gson.toJson(of<String, Any>(
                            "routing_key", bindingData.routing_key,
                            "arguments", bindingData.arguments)),
                            MediaType.APPLICATION_JSON_TYPE)),
                    Status.CREATED.statusCode)
        } else if (bindingData.destination_type == DestinationType.exchange) {
            expect(requestBuilder.nextWithAuthentication(user, pass).request(PATH_BINDING_EXCHANGE, of<String, String>(
                    "vhost", vhost,
                    "exchange", exchange,
                    "to", bindingData.destination)).post(entity(gson.toJson(of<String, Any>(
                    "routing_key", bindingData.routing_key,
                    "arguments", bindingData.arguments)),
                    MediaType.APPLICATION_JSON_TYPE)),
                    Status.CREATED.statusCode)
        } else {
            val error = "No destination_type specified for binding at $exchange@$vhost: ${bindingData.destination}"
            log.error(error)
            throw RestClientException(error)
        }
    }

    @Throws(RestClientException::class)
    private fun expect(response: Response, statusExpected: Int): Response {
        if (response.status != statusExpected) {
            val error = "Response with HTTP status %d %s, expected status code %d".format(response.status, response.statusInfo.reasonPhrase, statusExpected)
            log.error(error)
            throw RestClientException(error)
        }
        return response
    }

    @Throws(RestClientException::class)
    private fun expectOrEmpty(response: Response,
                              statusExpected: Int,
                              statusEmpty: Int): Optional<Response> {
        if (response.status == statusExpected) {
            return Optional.of(response)
        } else if (response.status == statusEmpty) {
            return Optional.empty<Response>()
        } else {
            val error = "Response with HTTP status %d %s, expected status code %d or %s".format(response.status, response.statusInfo.reasonPhrase, statusExpected, statusEmpty)
            log.error(error)
            throw RestClientException(error)
        }
    }


    @Throws(RestClientException::class)
    private fun <D> require(type: String, name: String, property: String, value: D?) {
        if (value == null) {
            val error = "$type $name missing required field: $property"
            log.error(error)
            throw RestClientException(error)
        }
    }
}
