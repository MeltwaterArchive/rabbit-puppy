package com.meltwater.puppy.rest

import com.google.common.collect.ImmutableMap.of
import com.google.gson.Gson
import com.insightfullogic.lambdabehave.JunitSuiteRunner
import com.insightfullogic.lambdabehave.Suite.describe
import com.meltwater.puppy.config.ExchangeData
import com.meltwater.puppy.config.ExchangeType
import com.meltwater.puppy.config.PermissionsData
import com.meltwater.puppy.config.VHostData
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_EXCHANGES_SINGLE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_PERMISSIONS_SINGLE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_VHOSTS_SINGLE
import org.hamcrest.Matchers.greaterThanOrEqualTo
import org.junit.runner.RunWith
import java.io.IOException
import java.util.*
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

@RunWith(JunitSuiteRunner::class)
class RabbitRestClientTest {
    init {
        val properties = object : Properties() {
            init {
                try {
                    load(ClassLoader.getSystemResourceAsStream("test.properties"))
                } catch (e: IOException) {
                    e.printStackTrace()
                }
            }
        }

        val gson = Gson()

        val brokerAddress = properties.getProperty("rabbit.broker.address")
        val brokerUser = properties.getProperty("rabbit.broker.user")
        val brokerPass = properties.getProperty("rabbit.broker.pass")

        val req = RestRequestBuilder(brokerAddress, Pair(brokerUser, brokerPass)).withHeader("content-type", "application/json")

        describe("a RabbitMQ REST client with valid auth credentials") { it ->

            val rabbitRestClient = RabbitRestClient(brokerAddress, brokerUser, brokerPass)

            it.isSetupWith {
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test")).put(Entity.entity(gson.toJson(VHostData()), MediaType.APPLICATION_JSON_TYPE))
                req.request(PATH_PERMISSIONS_SINGLE, of("vhost", "test", "user", "guest")).put(Entity.entity(gson.toJson(PermissionsData()), MediaType.APPLICATION_JSON_TYPE))
                req.request(PATH_EXCHANGES_SINGLE, of("vhost", "test", "exchange", "test.ex")).put(Entity.entity(gson.toJson(ExchangeData()), MediaType.APPLICATION_JSON_TYPE))
            }

            it.isConcludedWith {
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test")).delete()
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test1")).delete()
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test2")).delete()
                req.request(PATH_VHOSTS_SINGLE, of("vhost", "test/test")).delete()
            }

            it.uses("test1", VHostData(false)).and("test2", VHostData(true)).toShow("creates vhost: %s") { expect, vhost, data ->
                rabbitRestClient.createVirtualHost(vhost, data)

                val map = gson.fromJson<Map<Any, Any>>(getString(req, PATH_VHOSTS_SINGLE, of("vhost", vhost)), Map::class.java)
                expect.that(map["tracing"]).`is`(data.tracing)
            }

            it.should("gets existing vhosts") { expect ->
                val virtualHosts = rabbitRestClient.getVirtualHosts()
                expect.that(virtualHosts.keys).hasSize(greaterThanOrEqualTo(1)).hasItem("/")

                expect.that(virtualHosts["/"])
                        .isNotNull
                        .instanceOf(VHostData::class.java)
            }

            it.uses("ex1", "test", exchangeOfType(ExchangeType.fanout))
                    .and("ex2", "test", exchangeOfType(ExchangeType.direct))
                    .and("ex3", "test", exchangeOfType(ExchangeType.headers))
                    .and("ex4", "test", ExchangeData(ExchangeType.topic, false, true, true, of("foo", "bar")))
                    .toShow("creates exchange: %s") { expect, exchange, vhost, data ->
                rabbitRestClient.createExchange(vhost, exchange, data, brokerUser, brokerUser)

                val response = gson.fromJson(getString(req, PATH_EXCHANGES_SINGLE, of(
                        "vhost", vhost,
                        "exchange", exchange)),
                        ExchangeData::class.java)

                expect.that(response).`is`(data)
            }

            it.should("gets existing exchange") { expect ->
                val exchange = rabbitRestClient.getExchange("/", "amq.direct", brokerUser, brokerPass)
                expect.that(exchange.isPresent)
                        .`is`(true)
                        .and(exchange.get())
                        .isNotNull
            }

            it.should("does not get non-existing exchange") { expect ->
                val exchange = rabbitRestClient.getExchange("/", "amq.NOPE", brokerUser, brokerPass)
                expect.that(exchange.isPresent)
                        .`is`(false)
            }

            // TODO test get/create users, permissions, queues, bindings

        }
    }

    private fun exchangeOfType(type: ExchangeType): ExchangeData {
        val exchangeData = ExchangeData()
        exchangeData.type = type
        return exchangeData
    }

    private fun getString(requestBuilder: RestRequestBuilder, path: String, params: Map<String, String>): String {
        return requestBuilder.request(path, params).get().readEntity(String::class.java)
    }
}