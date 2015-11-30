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
import kotlin.Unit;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.insightfullogic.lambdabehave.Suite.describe;
import static com.meltwater.puppy.config.RabbitConfig.Companion;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static kotlin.Unit.INSTANCE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
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
            RabbitConfig rabbitConfig = new RabbitConfig().add(Companion.vhost(VHOST, vHostData -> {
                vHostData.setTracing(true);
                return INSTANCE;
            }));

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
                        .thenReturn(of(VHOST, new VHostData(!VHOST_DATA.getTracing())));

                expect.exception(RabbitPuppyException.class, () -> puppy.apply(rabbitConfig));

                verify(rabbitRestClient).getVirtualHosts();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy configured to create a user", it -> {

            UserData data = new UserData();
            RabbitConfig rabbitConfig = new RabbitConfig().add(Companion.user("dan", u -> INSTANCE));
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
            RabbitConfig rabbitConfig = new RabbitConfig().add(Companion.permissions("dan", "test", p -> INSTANCE));
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
            RabbitConfig rabbitConfig = new RabbitConfig().add(Companion.exchange("foo", "vhost", e -> {
                e.setType("topic");
                return INSTANCE;
            }));

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
            RabbitConfig rabbitConfig = new RabbitConfig().add(Companion.queue("queue", "test", q -> INSTANCE));
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

            BindingData data = new BindingData("q", "queue", "#", new HashMap<>());
            RabbitConfig rabbitConfig = new RabbitConfig().add(Companion.binding("ex", "test", b -> {
                b.setDestination(data.getDestination());
                b.setDestination_type(data.getDestination_type());
                b.setRouting_key(data.getRouting_key());
                return INSTANCE;
            }));
            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it.should("create binding if it doesn't exist", expect -> {
                when(rabbitRestClient.getBindings("test", USER, PASS)).thenReturn(new HashMap<>());

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getBindings("test", USER, PASS);
                verify(rabbitRestClient).createBinding("test", "ex", data, USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("doesn't create binding if it exists", expect -> {
                when(rabbitRestClient.getBindings("test", USER, PASS)).thenReturn(
                        of("ex", newArrayList(new BindingData("q", "queue", "#", new HashMap<>()))));

                puppy.apply(rabbitConfig);

                verify(rabbitRestClient).getBindings("test", USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });

            it.should("throw exception if binding exists with different config", expect -> {
                when(rabbitRestClient.getBindings("test", USER, PASS)).thenReturn(
                        of("ex", newArrayList(new BindingData("q", "queue", "#.asdf.#", null))));

                expect.exception(RabbitPuppyException.class, () -> puppy.apply(rabbitConfig));

                verify(rabbitRestClient).getBindings("test", USER, PASS);
                verify(rabbitRestClient).getUsername();
                verify(rabbitRestClient).getPassword();
                verifyNoMoreInteractions(rabbitRestClient);
            });
        });

        describe("a rabbit-puppy creating a resource on a new vhost", it -> {

            Function<Void, RabbitConfig> newRabbitConfig = v -> new RabbitConfig()
                    .add(Companion.user("userA", d -> {
                        d.setAdmin(true);
                        d.setPassword("passA");
                        return INSTANCE;
                    }))
                    .add(Companion.user("userB", d -> {
                        d.setAdmin(true);
                        d.setPassword("passB");
                        return INSTANCE;
                    }))
                    .add(Companion.user("userC", d -> {
                        d.setAdmin(true);
                        d.setPassword("passC");
                        return INSTANCE;
                    }))
                    .add(Companion.permissions("userA", "test", d -> {
                        d.setConfigure("exA.*");
                        return INSTANCE;
                    }))
                    .add(Companion.permissions("userB", "test", d -> {
                        d.setConfigure("exB.*");
                        return INSTANCE;
                    }))
                    .add(Companion.permissions("userC", "test", d -> {
                        d.setConfigure(".*exC.*");
                        return INSTANCE;
                    }));

            RabbitPuppy puppy = new RabbitPuppy(rabbitRestClient);

            it
                    .uses("exA", "userA", "passA")
                    .and("exB123abc", "userB", "passB")
                    .and("foo.exC_bar", "userC", "passC")
                    .toShow("creates resource %s with user with correct permissions: %s", (expect, exchange, user, pass) -> {
                        ExchangeData exchangeData = new ExchangeData();
                        RabbitConfig rabbitConfig = newRabbitConfig.apply(null)
                                .add(Companion.exchange(exchange, "test", d -> INSTANCE));

                        when(rabbitRestClient.getPermissions()).thenReturn(new HashMap<>());
                        when(rabbitRestClient.getUsers()).thenReturn(new HashMap<>());
                        when(rabbitRestClient.getExchange("test", exchange, user, pass)).thenReturn(empty());

                        puppy.apply(rabbitConfig);

                        verify(rabbitRestClient).createExchange("test", exchange, exchangeData, user, pass);
                    });
        });
    }
}