package com.meltwater.puppy;

import com.insightfullogic.lambdabehave.JunitSuiteRunner;
import com.meltwater.puppy.config.RabbitConfig;
import com.meltwater.puppy.config.reader.RabbitConfigReader;
import com.meltwater.puppy.rest.RabbitRestClient;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.File;

import static com.insightfullogic.lambdabehave.Suite.describe;

@RunWith(JunitSuiteRunner.class)
public class NiceJavaApiSpec {{

    describe("loading from yaml file", it -> {

        String configPath = ClassLoader.getSystemResource("noop.yaml").getPath();

        String USER = "user";
        String PASS = "pass";

        RabbitRestClient rabbitRestClient = Mockito.mock(RabbitRestClient.class);
        Mockito.when(rabbitRestClient.getUsername()).thenReturn(USER);
        Mockito.when(rabbitRestClient.getPassword()).thenReturn(PASS);

        // This test is mostly a compile-time test, meant to check that the Kotlin/Java integration works and looks nice.
        it.should("has a nice Java API", expect -> {
            RabbitConfig rabbitConfig = new RabbitConfigReader().read(new File(configPath));
            new RabbitPuppy(rabbitRestClient).apply(rabbitConfig);
        });
    });
}}
