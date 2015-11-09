package com.meltwater.puppy;

import com.insightfullogic.lambdabehave.JunitSuiteRunner;
import com.meltwater.puppy.config.BindingData;
import com.meltwater.puppy.config.ExchangeData;
import com.meltwater.puppy.config.PermissionsData;
import com.meltwater.puppy.config.QueueData;
import com.meltwater.puppy.config.RabbitConfig;
import com.meltwater.puppy.config.UserData;
import com.meltwater.puppy.config.VHostData;
import com.meltwater.puppy.rest.RabbitRestClient;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.insightfullogic.lambdabehave.Suite.describe;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(JunitSuiteRunner.class)
public class RabbitPuppySpec {
    {

        final String USER = "user";
        final String PASS = "pass";

        RabbitRestClient rabbitRestClient = mock(RabbitRestClient.class);
        when(rabbitRestClient.getUsername()).thenReturn(USER);
        when(rabbitRestClient.getPassword()).thenReturn(PASS);

        describe("a rabbit-puppy waiting for broker connection", it -> {
            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it.should("waits until connection available", expect -> {
                when(rabbitRestClient.ping())
                        .thenReturn(false)
                        .thenReturn(true);

                puppy.waitForBroker(3);

                verify(rabbitRestClient, times(2)).ping();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy configured to create a vhost", it -> {

            final String VHOST = "vhost";
            final VHostData VHOST_DATA = new VHostData(true);

            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);
            RabbitConfig rabbitConfig = new RabbitConfig().addVhost(VHOST, VHOST_DATA);

            it.should("creates vhost if it doesn't exist", expect -> {
                when(rabbitRestClient.getVirtualHosts())
                        .thenReturn(of());

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getVirtualHosts();
                verify(rabbitRestClient).createVirtualHost(VHOST, VHOST_DATA);
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("doesn't create vhost if it exists with same config", expect -> {
                when(rabbitRestClient.getVirtualHosts())
                        .thenReturn(of(VHOST, VHOST_DATA));

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getVirtualHosts();
                verifyNoMoreInteractions(rabbitRestClient);

            });

            it.should("throw exception if vhosts exists with different config", expect -> {
                when(rabbitRestClient.getVirtualHosts())
                        .thenReturn(of(VHOST, new VHostData(!VHOST_DATA.isTracing())));

                expect.exception(RabbitPuppyException.class, () -> puppy.apply(rabbitConfig));

                verify(rabbitRestClient).getVirtualHosts();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy configured to create a user", it -> {

            UserData data = new UserData();
            RabbitConfig rabbitConfig = new RabbitConfig()
                    .addUser("dan", data);
            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it.should("create it if it doesn't exist", expect -> {
                when(rabbitRestClient.getUsers()).thenReturn(new HashMap<>());

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getUsers();
                verify(rabbitRestClient).createUser("dan", data);
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("doesn't create it if it exists with same config", expect -> {
                when(rabbitRestClient.getUsers()).thenReturn(of("dan", new UserData()));

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getUsers();
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("throw exception if it exists with different config", expect -> {
                when(rabbitRestClient.getUsers()).thenReturn(of("dan", new UserData()));

                expect.exception(RabbitPuppyException.class, () -> puppy.apply(rabbitConfig));

                verify(rabbitRestClient).getUsers();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy configured to create permissions", it -> {

            PermissionsData data = new PermissionsData();
            RabbitConfig rabbitConfig = new RabbitConfig()
                    .addPermissions("dan", "test", data);
            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it.should("create it if it doesn't exist", expect -> {
                when(rabbitRestClient.getPermissions()).thenReturn(new HashMap<>());

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getPermissions();
                verify(rabbitRestClient).createPermissions("dan", "test", data);
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("doesn't create it if it exists with same config", expect -> {
                when(rabbitRestClient.getPermissions()).thenReturn(of("dan@test", new PermissionsData()));

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getPermissions();
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("throw exception if it exists with different config", expect -> {
                when(rabbitRestClient.getPermissions()).thenReturn(of("dan@test", new PermissionsData()));

                expect.exception(RabbitPuppyException.class, () -> puppy.apply(rabbitConfig));

                verify(rabbitRestClient).getPermissions();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy configured to create an exchange", it -> {

            final ExchangeData data = new ExchangeData("topic", true, false, false, new HashMap<>());

            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);
            RabbitConfig rabbitConfig = new RabbitConfig().addExchange("foo", "vhost", data);

            it.should("create exchange if it doesn't exist", expect -> {
                when(rabbitRestClient.getExchange("vhost", "foo", USER, PASS))
                        .thenReturn(Optional.<ExchangeData>empty());

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verify(rabbitRestClient).getExchange("vhost", "foo", USER, PASS);
                verify(rabbitRestClient).createExchange("vhost", "foo", data, USER, PASS);
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("doesn't create exchange if it exists with same config", expect -> {
                when(rabbitRestClient.getExchange("vhost", "foo", USER, PASS))
                        .thenReturn(of(data));

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verify(rabbitRestClient).getExchange("vhost", "foo", USER, PASS);
                verifyNoMoreInteractions(rabbitRestClient);

            });

            it.should("throw exception if exchange exists with different config", expect -> {
                when(rabbitRestClient.getExchange("vhost", "foo", USER, PASS))
                        .thenReturn(of(new ExchangeData().addArgument("foo", "bar")));

                expect.exception(RabbitPuppyException.class, () -> puppy.apply(rabbitConfig));

                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verify(rabbitRestClient).getExchange("vhost", "foo", USER, PASS);
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy configured to create a queue", it -> {

            QueueData data = new QueueData();
            RabbitConfig rabbitConfig = new RabbitConfig()
                    .addQueue("queue", "test", data);
            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it.should("create it if it doesn't exist", expect -> {
                when(rabbitRestClient.getQueue("test", "queue", USER, PASS)).thenReturn(empty());

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getQueue("test", "queue", USER, PASS);
                verify(rabbitRestClient).createQueue("test", "queue", data, USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("doesn't create it if it exists with same config", expect -> {
                when(rabbitRestClient.getQueue("test", "queue", USER, PASS)).thenReturn(of(new QueueData()));

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getQueue("test", "queue", USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("throw exception if it exists with different config", expect -> {
                when(rabbitRestClient.getQueue("test", "queue", USER, PASS))
                        .thenReturn(of(new QueueData(false, true, new HashMap<>())));

                expect.exception(RabbitPuppyException.class, () -> puppy.apply(rabbitConfig));

                verify(rabbitRestClient).getQueue("test", "queue", USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy configured to create a binding", it -> {

            BindingData data = new BindingData("q", "queue", "#", null);
            RabbitConfig rabbitConfig = new RabbitConfig()
                    .addBinding("ex", "test", data);
            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it.should("create it if it doesn't exist", expect -> {
                when(rabbitRestClient.getBindings("test", USER, PASS)).thenReturn(new HashMap<>());

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getBindings("test", USER, PASS);
                verify(rabbitRestClient).createBinding("test", "ex", data, USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("doesn't create it if it exists", expect -> {
                when(rabbitRestClient.getBindings("test", USER, PASS)).thenReturn(
                        of("ex", newArrayList(new BindingData("q", "queue", "#", null))));

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getBindings("test", USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy creating a resource on a new vhost", it -> {

            Function<Void, RabbitConfig> rabbitConfigFunction = v -> new RabbitConfig()
                    .addUser("userA", new UserData("passA", true))
                    .addPermissions("userA", "test", new PermissionsData("exA.*", "", ""))
                    .addUser("userB", new UserData("passB", true))
                    .addPermissions("userB", "test", new PermissionsData("exB.*", "", ""))
                    .addUser("userC", new UserData("passC", true))
                    .addPermissions("userC", "test", new PermissionsData(".*exC.*", "", ""));

            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it
                    .uses("exA", "userA", "passA")
                    .and("exB123abc", "userB", "passB")
                    .and("foo.exC_bar", "userC", "passC")
                    .toShow("creates resource %s with user with correct permissions: %s", (expect, exchange, user, pass) -> {
                        ExchangeData exchangeData = mock(ExchangeData.class);
                        RabbitConfig rabbitConfig = rabbitConfigFunction.apply(null)
                                .addExchange(exchange, "test", exchangeData);

                        when(rabbitRestClient.getPermissions()).thenReturn(new HashMap<>());
                        when(rabbitRestClient.getUsers()).thenReturn(new HashMap<>());
                        when(rabbitRestClient.getExchange("test", exchange, user, pass)).thenReturn(empty());

                        puppy.apply(rabbitConfig);

                        verify(rabbitRestClient).createExchange("test", exchange, exchangeData, user, pass);
                    });
        });
    }
}