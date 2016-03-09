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
import org.hamcrest.Matchers
import org.junit.runner.RunWith
import java.io.IOException
import java.util.*

@RunWith(JunitSuiteRunner::class)
class VerifyEndToEndTest {
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

        describe("a rabbit-puppy verifying configuration on an empty external broker") { it ->

            fun verify() = Run().run("rabbit-puppy", arrayOf("verify",
                    "--broker", brokerAddress,
                    "--user", brokerUser,
                    "--pass", brokerPass,
                    "--config", configPath))

            it.isConcludedWith {
                // Clean up just in case this test applies anything
                req.request(PATH_VHOSTS_SINGLE, of("vhost", VHOST)).delete()
                req.request(PATH_USERS_SINGLE, of("user", "test_dan")).delete()
            }

            it.should("encounters errors") { expect ->
                expect.that(verify()).`is`(false)
            }

            it.should("creates no vhost") { expect ->
                verify()
                val map = gson.fromJson<Map<Any, Any>>(getString(req, PATH_VHOSTS_SINGLE, of(
                        "vhost", VHOST)),
                        Map::class.java)

                expect.that(map).has(Matchers.hasEntry("error", "Object Not Found"))
            }

            it.should("creates no user") { expect ->
                verify()
                val map = gson.fromJson<Map<Any, Any>>(getString(req, PATH_USERS_SINGLE, of(
                        "user", "test_dan")),
                        Map::class.java)

                expect.that(map).has(Matchers.hasEntry("error", "Object Not Found"))
            }

        }
    }

    private fun getString(requestBuilder: RestRequestBuilder, path: String, params: Map<String, String>): String {
        return requestBuilder.request(path, params).get().readEntity(String::class.java)
    }
}
