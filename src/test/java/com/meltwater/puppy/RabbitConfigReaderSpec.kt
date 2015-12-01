package com.meltwater.puppy

import com.insightfullogic.lambdabehave.JunitSuiteRunner
import com.meltwater.puppy.config.BindingData
import com.meltwater.puppy.config.ExchangeData
import com.meltwater.puppy.config.ExchangeType
import com.meltwater.puppy.config.PermissionsData
import com.meltwater.puppy.config.QueueData
import com.meltwater.puppy.config.RabbitConfig
import com.meltwater.puppy.config.UserData
import com.meltwater.puppy.config.VHostData
import com.meltwater.puppy.config.reader.RabbitConfigException
import com.meltwater.puppy.config.reader.RabbitConfigReader
import org.junit.runner.RunWith

import java.io.File
import java.util.HashMap

import com.google.common.collect.ImmutableMap.of
import com.insightfullogic.lambdabehave.Suite.describe
import org.hamcrest.Matchers.hasEntry

@SuppressWarnings("ConstantConditions")
@RunWith(JunitSuiteRunner::class)
class RabbitConfigReaderSpec {
    init {

        val rabbitConfigReader = RabbitConfigReader()

        val configFile = File(ClassLoader.getSystemResource("rabbitconfig.yaml").file)
        val configFileBad = File(ClassLoader.getSystemResource("rabbitconfig.bad.yaml").file)

        describe("a RabbitConfigReader reading valid config file ") { it ->
            val config: RabbitConfig
            try {
                config = rabbitConfigReader.read(configFile)
            } catch (e: RabbitConfigException) {
                throw RuntimeException(e)
            }

            it.should("reads vhosts") { expect ->
                expect
                        .that(config.vhosts.size)
                        .`is`(3)
                        .and(config.vhosts)
                        .has(hasEntry("input", VHostData(true)))
                        .has(hasEntry("output", VHostData(false)))
                        .has(hasEntry<String, VHostData>("test", null))
            }

            it.should("reads users") { expect ->
                expect
                        .that(config.users.size)
                        .`is`(2)
                        .and(config.users)
                        .has(hasEntry("test_dan", UserData("torrance", true)))
                        .has(hasEntry("test_jack", UserData("bauer", false)))
            }

            it.should("reads permissions") { expect ->
                expect
                        .that(config.permissions.size)
                        .`is`(2)
                        .and(config.permissions)
                        .has(hasEntry("test_dan@input", PermissionsData(".*", ".*", ".*")))
                        .has(hasEntry("test_dan@output", PermissionsData(".*", ".*", ".*")))
            }

            it.should("reads exchanges") { expect ->
                expect
                        .that(config.exchanges.size)
                        .`is`(3)
                        .and(config.exchanges)
                        .has(hasEntry("exchange.in@input", ExchangeData(ExchangeType.topic, false, true, true, of<String, Any>("hash-header", "abc"))))
                        .has(hasEntry("exchange.out@output", ExchangeData(ExchangeType.fanout, true, false, false)))
                        .has(hasEntry("exchange.out.direct@output", ExchangeData(ExchangeType.direct, true, false, false)))
            }

            it.should("reads queues") { expect ->
                expect
                        .that(config.queues.size)
                        .`is`(2)
                        .and(config.queues)
                        .has(hasEntry("queue-in@input", QueueData(false, true, HashMap<String, Any>())
                                .addArgument("x-message-ttl", 123)
                                .addArgument("x-dead-letter-exchange", "other")))
                        .has(hasEntry("queue-out@output", QueueData(true, false)))
            }

            it.should("reads bindings") { expect ->
                expect
                        .that(config.bindings.size)
                        .`is`(2)
                        .and(config.bindings["exchange.in@input"])
                        .hasSize(1)
                        .contains(BindingData("queue-in", "queue", "#", of<String, Any>("foo", "bar")))
                        .and(config.bindings["exchange.out@output"])
                        .hasSize(2)
                        .containsInAnyOrder(
                                BindingData("queue-out", "queue", ""),
                                BindingData("exchange.out.direct", "exchange", "#"))
            }
        }

        describe("a RabbitConfigReader reading invalid config file") { it ->

            it.should("fails nicely") { expect -> expect.exception(RabbitConfigException::class.java) { rabbitConfigReader.read(configFileBad) } }
        }

    }
}
