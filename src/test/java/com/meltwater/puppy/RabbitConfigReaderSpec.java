package com.meltwater.puppy;

import com.insightfullogic.lambdabehave.JunitSuiteRunner;
import com.meltwater.puppy.config.BindingData;
import com.meltwater.puppy.config.ExchangeData;
import com.meltwater.puppy.config.PermissionsData;
import com.meltwater.puppy.config.QueueData;
import com.meltwater.puppy.config.RabbitConfig;
import com.meltwater.puppy.config.UserData;
import com.meltwater.puppy.config.VHostData;
import com.meltwater.puppy.config.reader.RabbitConfigException;
import com.meltwater.puppy.config.reader.RabbitConfigReader;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;

import static com.google.common.collect.ImmutableMap.of;
import static com.insightfullogic.lambdabehave.Suite.describe;
import static org.hamcrest.Matchers.hasEntry;

@SuppressWarnings("ConstantConditions")
@RunWith(JunitSuiteRunner.class)
public class RabbitConfigReaderSpec {{

    RabbitConfigReader rabbitConfigReader = new RabbitConfigReader();

    File configFile = new File(ClassLoader.getSystemResource("rabbitconfig.yaml").getFile());
    File configFileBad = new File(ClassLoader.getSystemResource("rabbitconfig.bad.yaml").getFile());

    describe("a RabbitConfigReader reading valid config file ", it -> {
        final RabbitConfig config;
        try {
            config = rabbitConfigReader.read(configFile);
        } catch (RabbitConfigException e) {
            throw new RuntimeException(e);
        }

        it.should("reads vhosts", expect -> {
            expect
                    .that(config.getVhosts().size())
                    .is(3)
                    .and(config.getVhosts())
                    .has(hasEntry("input", new VHostData(true)))
                    .has(hasEntry("output", new VHostData(false)))
                    .has(hasEntry("test", null));
        });


        it.should("reads users", expect -> {
            expect
                    .that(config.getUsers().size())
                    .is(2)
                    .and(config.getUsers())
                    .has(hasEntry("test_dan", new UserData("torrance", true)))
                    .has(hasEntry("test_jack", new UserData("bauer", false)));

        });

        it.should("reads permissions", expect -> {
            expect
                    .that(config.getPermissions().size())
                    .is(2)
                    .and(config.getPermissions())
                    .has(hasEntry("test_dan@input", new PermissionsData(".*", ".*", ".*")))
                    .has(hasEntry("test_dan@output", new PermissionsData(".*", ".*", ".*")));
        });

        it.should("reads exchanges", expect -> {
            expect
                    .that(config.getExchanges().size())
                    .is(3)
                    .and(config.getExchanges())
                    .has(hasEntry("exchange.in@input", new ExchangeData("topic", false, true, true, of("hash-header", "abc"))))
                    .has(hasEntry("exchange.out@output", new ExchangeData("fanout", true, false, false, null)))
                    .has(hasEntry("exchange.out.direct@output", new ExchangeData("direct", true, false, false, null)));
        });

        it.should("reads queues", expect -> {
            expect
                    .that(config.getQueues().size())
                    .is(2)
                    .and(config.getQueues())
                    .has(hasEntry("queue-in@input", new QueueData(false, true, new HashMap<>())
                            .addArgument("x-message-ttl", 123)
                            .addArgument("x-dead-letter-exchange", "other")))
                    .has(hasEntry("queue-out@output", new QueueData(true, false, null)));
        });

        it.should("reads bindings", expect -> {
            expect
                    .that(config.getBindings().size())
                    .is(2)
                    .and(config.getBindings().get("exchange.in@input"))
                    .hasSize(1)
                    .contains(new BindingData("queue-in", "queue", "#", of("foo", "bar")))
                    .and(config.getBindings().get("exchange.out@output"))
                    .hasSize(2)
                    .containsInAnyOrder(
                            new BindingData("queue-out", "queue", "", null),
                            new BindingData("exchange.out.direct", "exchange", "#", null));
        });
    });

    describe("a RabbitConfigReader reading invalid config file", it -> {

        it.should("fails nicely", expect -> {
            expect.exception(RabbitConfigException.class, () ->
                            rabbitConfigReader.read(configFileBad)
            );
        });
    });

}}
