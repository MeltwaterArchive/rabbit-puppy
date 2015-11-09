package com.meltwater.puppy;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.meltwater.puppy.config.BindingData;
import com.meltwater.puppy.config.ExchangeData;
import com.meltwater.puppy.config.PermissionsData;
import com.meltwater.puppy.config.QueueData;
import com.meltwater.puppy.config.RabbitConfig;
import com.meltwater.puppy.config.UserData;
import com.meltwater.puppy.config.VHostData;
import com.meltwater.puppy.config.reader.RabbitConfigException;
import com.meltwater.puppy.config.reader.RabbitConfigReader;
import com.meltwater.puppy.rest.RabbitRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final RabbitConfigReader rabbitConfigReader = new RabbitConfigReader();

    private static class Arguments {
        @Parameter(names = { "-h", "--help"}, description = "Print help and exit", help = true)
        private boolean help;

        @Parameter(names = { "-c", "--config" }, description = "YAML config file path", required = true)
        private String configPath;

        @Parameter(names = { "-b", "--broker" }, description = "HTTP URL to broker", required = true)
        private String broker;

        @Parameter(names = { "-u", "--user" }, description = "Username", required = true)
        private String user;

        @Parameter(names = { "-p", "--pass" }, description = "Password", required = true)
        private String pass;

        @Parameter(names = { "-w", "--wait" }, description = "Seconds to wait for broker to become available")
        private int wait = 0;
    }

    public static void main(String[] argv) throws IOException {
        Arguments arguments = parseArguments("rabbit-puppy", argv);
        log.info("Reading configuration from " + arguments.configPath);
        try {
            RabbitConfig rabbitConfig = rabbitConfigReader.read(new File(arguments.configPath));
            RabbitPuppy rabbitPuppy = new RabbitPuppy(arguments.broker, arguments.user, arguments.pass);
            if (arguments.wait > 0) {
                rabbitPuppy.waitForBroker(arguments.wait);
            }
            rabbitPuppy.apply(rabbitConfig);
        } catch (RabbitConfigException e) {
            log.error("Failed to read configuration, exiting");
            System.exit(1);
        } catch (RabbitPuppyException e) {
            log.error(String.format("Encountered %d errors, exiting", e.getErrors().size()));
            System.exit(1);
        }
    }

    private static Arguments parseArguments(String programName, String[] argv) {
        Arguments arguments = new Arguments();
        JCommander jc = new JCommander();
        jc.addObject(arguments);
        jc.setProgramName(programName);

        try {
            jc.parse(argv);
            if (arguments.help) {
                jc.usage();
                System.exit(1);
            }
        }
        catch (ParameterException e) {
            log.error(e.getMessage());
            jc.usage();
            System.exit(1);
        }
        return arguments;
    }
}
