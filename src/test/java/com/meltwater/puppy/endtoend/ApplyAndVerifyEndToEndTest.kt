package com.meltwater.puppy.endtoend

import com.google.common.collect.ImmutableMap
import com.google.gson.Gson
import com.insightfullogic.lambdabehave.JunitSuiteRunner
import com.insightfullogic.lambdabehave.Suite
import com.meltwater.puppy.Run
import com.meltwater.puppy.rest.RabbitRestClient
import com.meltwater.puppy.rest.RabbitRestClient.Companion.PATH_VHOSTS_SINGLE
import com.meltwater.puppy.rest.RestRequestBuilder
import org.junit.runner.RunWith
import java.io.IOException
import java.util.Properties

@RunWith(JunitSuiteRunner::class)
public class ApplyAndVerifyEndToEndTest {
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

        Suite.describe("a rabbit-puppy applying and verifying configuration on external rabbit") { it ->

            it.isSetupWith { Run().run("rabbit-puppy",arrayOf("apply",
                    "--broker", brokerAddress,
                    "--user", brokerUser,
                    "--pass", brokerPass,
                    "--config", configPath)) }

            it.isConcludedWith {
                req.request(PATH_VHOSTS_SINGLE, ImmutableMap.of("vhost", VHOST)).delete()
                req.request(RabbitRestClient.PATH_USERS_SINGLE, ImmutableMap.of("user", "test_dan")).delete()
            }

            fun verify() = Run().run("rabbit-puppy", arrayOf("verify",
                    "--broker", brokerAddress,
                    "--user", brokerUser,
                    "--pass", brokerPass,
                    "--config", configPath))

            it.should("creates something") { expect ->
                val map = gson.fromJson<Map<Any, Any>>(getString(req, PATH_VHOSTS_SINGLE, ImmutableMap.of(
                        "vhost", VHOST)),
                        Map::class.java)

                expect.that(map["name"]).`is`(VHOST)
            }

            it.should("successfully verifies the created config") { expect ->
                expect.that(verify()).`is`(true)
            }
        }
    }

    private fun getString(requestBuilder: RestRequestBuilder, path: String, params: Map<String, String>): String {
        return requestBuilder.request(path, params).get().readEntity(String::class.java)
    }
}
