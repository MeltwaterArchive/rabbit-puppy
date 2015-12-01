package com.meltwater.puppy

import com.google.common.collect.ImmutableMap.of
import com.google.common.collect.Lists.newArrayList
import com.insightfullogic.lambdabehave.JunitSuiteRunner
import com.insightfullogic.lambdabehave.Suite.describe
import com.meltwater.puppy.config.*
import com.meltwater.puppy.rest.RabbitRestClient
import org.junit.runner.RunWith
import org.mockito.Mockito.*
import java.util.*
import java.util.Optional.empty
import java.util.Optional.of

@RunWith(JunitSuiteRunner::class)
class RabbitPuppySpec {
    init {

        val USER = "user"
        val PASS = "pass"

        var rabbitRestClient: RabbitRestClient = mock(RabbitRestClient::class.java)
        `when`(rabbitRestClient.getUsername()).thenReturn(USER)
        `when`(rabbitRestClient.getPassword()).thenReturn(PASS)

        describe("a rabbit-puppy waiting for broker connection") { it ->
            val puppy = RabbitPuppy(rabbitRestClient)

            it.should("waits until connection available") { expect ->
                `when`(rabbitRestClient.ping()).thenReturn(false).thenReturn(true)

                puppy.waitForBroker(3)

                verify(rabbitRestClient, times(2)).ping()
                verifyNoMoreInteractions(rabbitRestClient)
            }
        }

        describe("a rabbit-puppy configured to create a vhost") { it ->

            val VHOST = "vhost"
            val VHOST_DATA = VHostData(true)

            val puppy = RabbitPuppy(rabbitRestClient)
            val rabbitConfig = RabbitConfig().addVhost(VHOST) { tracing = true }

            it.should("creates vhost if it doesn't exist") { expect ->
                `when`(rabbitRestClient.getVirtualHosts()).thenReturn(of<String, VHostData>())

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getVirtualHosts()
                verify(rabbitRestClient).createVirtualHost(VHOST, VHOST_DATA)
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("doesn't create vhost if it exists with same config") { expect ->
                `when`(rabbitRestClient.getVirtualHosts()).thenReturn(of(VHOST, VHOST_DATA))

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getVirtualHosts()
                verifyNoMoreInteractions(rabbitRestClient)

            }

            it.should("throw exception if vhosts exists with different config") { expect ->
                `when`(rabbitRestClient.getVirtualHosts()).thenReturn(of(VHOST, VHostData(!VHOST_DATA.tracing)))

                expect.exception(RabbitPuppyException::class.java) { puppy.apply(rabbitConfig) }

                verify(rabbitRestClient).getVirtualHosts()
                verifyNoMoreInteractions(rabbitRestClient)
            }
        }

        describe("a rabbit-puppy configured to create a user") { it ->

            val data = UserData()
            val rabbitConfig = RabbitConfig().addUser("dan") {}
            val puppy = RabbitPuppy(rabbitRestClient)

            it.should("create it if it doesn't exist") { expect ->
                `when`(rabbitRestClient.getUsers()).thenReturn(HashMap<String, UserData>())

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getUsers()
                verify(rabbitRestClient).createUser("dan", data)
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("doesn't create it if it exists with same config") { expect ->
                `when`(rabbitRestClient.getUsers()).thenReturn(of("dan", UserData()))

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getUsers()
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("throw exception if it exists with different config") { expect ->
                `when`(rabbitRestClient.getUsers()).thenReturn(of("dan", UserData()))

                expect.exception(RabbitPuppyException::class.java) { puppy.apply(rabbitConfig) }

                verify(rabbitRestClient).getUsers()
                verifyNoMoreInteractions(rabbitRestClient)
            }
        }

        describe("a rabbit-puppy configured to create permissions") { it ->

            val data = PermissionsData()
            val rabbitConfig = RabbitConfig().addPermissions("dan", "test") {}
            val puppy = RabbitPuppy(rabbitRestClient)

            it.should("create it if it doesn't exist") { expect ->
                `when`(rabbitRestClient.getPermissions()).thenReturn(HashMap<String, PermissionsData>())

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getPermissions()
                verify(rabbitRestClient).createPermissions("dan", "test", data)
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("doesn't create it if it exists with same config") { expect ->
                `when`(rabbitRestClient.getPermissions()).thenReturn(of("dan@test", PermissionsData()))

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getPermissions()
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("throw exception if it exists with different config") { expect ->
                `when`(rabbitRestClient.getPermissions()).thenReturn(of("dan@test", PermissionsData()))

                expect.exception(RabbitPuppyException::class.java) { puppy.apply(rabbitConfig) }

                verify(rabbitRestClient).getPermissions()
                verifyNoMoreInteractions(rabbitRestClient)
            }
        }

        describe("a rabbit-puppy configured to create an exchange") { it ->

            val data = ExchangeData(ExchangeType.topic, true, false, false, HashMap<String, Any>())

            val puppy = RabbitPuppy(rabbitRestClient)
            val rabbitConfig = RabbitConfig().addExchange("foo", "vhost") { type = ExchangeType.topic }

            it.should("create exchange if it doesn't exist") { expect ->
                `when`(rabbitRestClient.getExchange("vhost", "foo", USER, PASS)).thenReturn(Optional.empty<ExchangeData>())

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verify(rabbitRestClient).getExchange("vhost", "foo", USER, PASS)
                verify(rabbitRestClient).createExchange("vhost", "foo", data, USER, PASS)
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("doesn't create exchange if it exists with same config") { expect ->
                `when`(rabbitRestClient.getExchange("vhost", "foo", USER, PASS)).thenReturn(of(data))

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verify(rabbitRestClient).getExchange("vhost", "foo", USER, PASS)
                verifyNoMoreInteractions(rabbitRestClient)

            }

            it.should("throw exception if exchange exists with different config") { expect ->
                `when`(rabbitRestClient.getExchange("vhost", "foo", USER, PASS)).thenReturn(of(ExchangeData().addArgument("foo", "bar")))

                expect.exception(RabbitPuppyException::class.java) { puppy.apply(rabbitConfig) }

                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verify(rabbitRestClient).getExchange("vhost", "foo", USER, PASS)
                verifyNoMoreInteractions(rabbitRestClient)
            }
        }

        describe("a rabbit-puppy configured to create a queue") { it ->

            val data = QueueData()
            val rabbitConfig = RabbitConfig().addQueue("queue", "test") { }
            val puppy = RabbitPuppy(rabbitRestClient)

            it.should("create it if it doesn't exist") { expect ->
                `when`(rabbitRestClient.getQueue("test", "queue", USER, PASS)).thenReturn(empty<QueueData>())

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getQueue("test", "queue", USER, PASS)
                verify(rabbitRestClient).createQueue("test", "queue", data, USER, PASS)
                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("doesn't create it if it exists with same config") { expect ->
                `when`(rabbitRestClient.getQueue("test", "queue", USER, PASS)).thenReturn(of(QueueData()))

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getQueue("test", "queue", USER, PASS)
                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("throw exception if it exists with different config") { expect ->
                `when`(rabbitRestClient.getQueue("test", "queue", USER, PASS)).thenReturn(of(QueueData(false, true, HashMap<String, Any>())))

                expect.exception(RabbitPuppyException::class.java) { puppy.apply(rabbitConfig) }

                verify(rabbitRestClient).getQueue("test", "queue", USER, PASS)
                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verifyNoMoreInteractions(rabbitRestClient)
            }
        }

        describe("a rabbit-puppy configured to create a binding") { it ->

            val data = BindingData("q", "queue", "#", HashMap<String, Any>())
            val rabbitConfig = RabbitConfig().addBinding("ex", "test") {
                destination = data.destination
                destination_type = data.destination_type
                routing_key = data.routing_key
            }
            val puppy = RabbitPuppy(rabbitRestClient)

            it.should("create it if it doesn't exist") { expect ->
                `when`(rabbitRestClient.getBindings("test", USER, PASS)).thenReturn(HashMap<String, List<BindingData>>())

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getBindings("test", USER, PASS)
                verify(rabbitRestClient).createBinding("test", "ex", data, USER, PASS)
                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verifyNoMoreInteractions(rabbitRestClient)
            }

            it.should("doesn't create it if it exists") { expect ->
                `when`(rabbitRestClient.getBindings("test", USER, PASS)).thenReturn(
                        of<String, List<BindingData>>("ex", newArrayList(BindingData("q", "queue", "#", HashMap<String, Any>()))))

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).getBindings("test", USER, PASS)
                verify(rabbitRestClient).getUsername()
                verify(rabbitRestClient).getPassword()
                verifyNoMoreInteractions(rabbitRestClient)
            }
        }

        describe("a rabbit-puppy creating a resource on a new vhost") { it ->

            fun newRabbitConfig() = RabbitConfig()
                    .addUser("userA") {
                        admin = true
                        password = "passA"
                    }.addUser("userB") {
                        admin = true
                        password = "passB"
                    }.addUser("userC") {
                        admin = true
                        password = "passC"
                    }.addPermissions("userA", "test") {
                        configure = "exA.*"
                    }.addPermissions("userB", "test") {
                        configure = "exB.*"
                    }.addPermissions("userC", "test") {
                        configure = ".*exC.*"
                    }

            val puppy = RabbitPuppy(rabbitRestClient)

            it.uses("exA", "userA", "passA").and("exB123abc", "userB", "passB").and("foo.exC_bar", "userC", "passC").toShow("creates resource %s with user with correct permissions: %s") { expect, exchange, user, pass ->
                val exchangeData = ExchangeData()
                val rabbitConfig = newRabbitConfig().addExchange(exchange, "test") { }

                `when`(rabbitRestClient.getPermissions()).thenReturn(HashMap<String, PermissionsData>())
                `when`(rabbitRestClient.getUsers()).thenReturn(HashMap<String, UserData>())
                `when`(rabbitRestClient.getExchange("test", exchange, user, pass)).thenReturn(empty<ExchangeData>())

                puppy.apply(rabbitConfig)

                verify(rabbitRestClient).createExchange("test", exchange, exchangeData, user, pass)
            }
        }
    }
}