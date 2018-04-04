package com.meltwater.puppy.endtoend

import com.google.common.collect.ImmutableMap.of
import com.google.gson.Gson
import com.insightfullogic.lambdabehave.JunitSuiteRunner
import com.insightfullogic.lambdabehave.Suite.describe
import com.meltwater.puppy.Run
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_BINDING_EXCHANGE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_BINDING_QUEUE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_EXCHANGES_SINGLE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_PERMISSIONS_SINGLE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_QUEUES_SINGLE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_USERS_SINGLE
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_VHOSTS_SINGLE
import com.meltwater.puppy.rest.RestRequestBuilder
import org.junit.runner.RunWith
import java.io.IOException
import java.util.*

@RunWith(JunitSuiteRunner::class)
class ApplyEndToEndTest {
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

        val VHOST = "endtoend"

        val req = RestRequestBuilder(brokerAddress, Pair(brokerUser, brokerPass))
                .withHeader("content-type", "application/json")

        val configPath = ClassLoader.getSystemResource("endtoend.yaml").path

        describe("a rabbit-puppy applying configuration on external rabbit") { it ->

            it.isSetupWith { Run().run("rabbit-puppy",arrayOf("apply",
                    "--broker", brokerAddress,
                    "--user", brokerUser,
                    "--pass", brokerPass,
                    "--config", configPath)) }

            it.isConcludedWith {
                req.request(PATH_VHOSTS_SINGLE, of("vhost", VHOST)).delete()
                req.request(PATH_USERS_SINGLE, of("user", "test_dan")).delete()
            }

            it.should("creates vhost") { expect ->
                val map = gson.fromJson<Map<*, *>>(getString(req, PATH_VHOSTS_SINGLE, of(
                        "vhost", VHOST)),
                        Map::class.java)

                expect.that(map["name"]).`is`(VHOST)
            }

            it.should("creates user") { expect ->
                val map = gson.fromJson<Map<*, *>>(getString(req, PATH_USERS_SINGLE, of(
                        "user", "test_dan")),
                        Map::class.java)

                expect.that(map["tags"]).`is`("administrator")
            }

            it.should("creates permissions") { expect ->
                val map = gson.fromJson<Map<*, *>>(getString(req, PATH_PERMISSIONS_SINGLE, of(
                        "vhost", VHOST,
                        "user", "test_dan")),
                        Map::class.java)

                expect.that(map["configure"]).`is`(".*")
                        .and(map["write"]).`is`(".*")
                        .and(map["read"]).`is`(".*")
            }

            it.should("creates exchange 'exchange.in@test'") { expect ->
                val map = gson.fromJson<Map<*, *>>(getString(
                        req.nextWithAuthentication("test_dan", "torrance"),
                        PATH_EXCHANGES_SINGLE,
                        of(
                                "exchange", "exchange.in",
                                "vhost", VHOST)),
                        Map::class.java)

                expect.that(map["type"]).`is`("topic")
                        .and(map["durable"]).`is`(false)
                        .and(map["auto_delete"]).`is`(true)
                        .and(map["internal"]).`is`(true)
                        .and((map["arguments"] as Map<*, *>).size).`is`(1)
                        .and((map["arguments"] as Map<*, *>)["hash-header"]).`is`("abc")
            }

            it.should("creates exchange 'exchange.out@test'") { expect ->
                val map = gson.fromJson<Map<*, *>>(getString(
                        req.nextWithAuthentication("test_dan", "torrance"),
                        PATH_EXCHANGES_SINGLE,
                        of(
                                "exchange", "exchange.out",
                                "vhost", VHOST)),
                        Map::class.java)

                expect.that(map["type"]).`is`("direct")
                        .and(map["durable"]).`is`(true)
                        .and(map["auto_delete"]).`is`(false)
                        .and(map["internal"]).`is`(false)
                        .and((map["arguments"] as Map<*, *>).size).`is`(0)
            }

            it.should("creates queue") { expect ->
                val map: Map<*, *> = gson.fromJson<Map<*, *>>(getString(
                        req.nextWithAuthentication("test_dan", "torrance"),
                        PATH_QUEUES_SINGLE,
                        of(
                                "queue", "queue-test",
                                "vhost", VHOST)),
                        Object::class.java)

                expect.that(map["durable"]).`is`(false)
                        .and(map["auto_delete"]).`is`(true)
                        .and((map["arguments"] as Map<*, *>).size).`is`(1)
                        .and((map["arguments"] as Map<*, *>)["x-message-ttl"]).isIn(123, 123.0)
            }

            it.should("creates binding to queue") { expect ->
                val map = gson.fromJson<List<Any>>(getString(
                        req.nextWithAuthentication("test_dan", "torrance"),
                        PATH_BINDING_QUEUE,
                        of(
                                "exchange", "exchange.in",
                                "to", "queue-test",
                                "vhost", VHOST)),
                        List::class.java)[0] as Map<*, *>

                expect.that(map["routing_key"]).`is`("route-queue")
                        .and((map["arguments"] as Map<*, *>).size).`is`(1)
                        .and((map["arguments"] as Map<*, *>)["foo"]).`is`("bar")
            }

            it.should("creates binding to exchange") { expect ->
                val map = gson.fromJson<List<Any>>(getString(
                        req.nextWithAuthentication("test_dan", "torrance"),
                        PATH_BINDING_EXCHANGE,
                        of(
                                "exchange", "exchange.in",
                                "to", "exchange.out",
                                "vhost", VHOST)),
                        List::class.java)[0] as Map<*, *>

                expect.that(map["routing_key"]).`is`("route-exchange")
                        .and((map["arguments"] as Map<*, *>).size).`is`(1)
                        .and((map["arguments"] as Map<*, *>)["cat"]).`is`("dog")
            }
        }
    }

    private fun getString(requestBuilder: RestRequestBuilder, path: String, params: Map<String, String>): String {
        return requestBuilder.request(path, params).get().readEntity(String::class.java)
    }
}
