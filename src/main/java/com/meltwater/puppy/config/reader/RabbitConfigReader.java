package com.meltwater.puppy.config.reader;

import com.google.common.base.Joiner;
import com.meltwater.puppy.config.BindingData;
import com.meltwater.puppy.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.constructor.ConstructorException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RabbitConfigReader {

    private static final Logger log = LoggerFactory.getLogger(RabbitConfigReader.class);

    public RabbitConfig read(String yaml) throws RabbitConfigException {
        try {
            return parseBindings(new Yaml(new Constructor(RabbitConfig.class)).loadAs(yaml, RabbitConfig.class));
        } catch (ConstructorException e) {
            log.error("Failed reading configuration: " + e.getMessage());
            throw new RabbitConfigException("Failed reading configuration", e);
        }
    }

    public RabbitConfig read(File yamlFile) throws RabbitConfigException {
        try {
            List<String> lines = Files.readAllLines(Paths.get(yamlFile.getAbsolutePath()));
            return read(Joiner.on('\n').join(lines));
        } catch (IOException e) {
            log.error("Failed reading from file " + yamlFile.getPath(), e.getMessage());
            throw new RabbitConfigException("Failed reading from file " + yamlFile.getPath(), e);
        }
    }

    /**
     * SnakeYAML fails to figure out Bindings types correctly, so we need to fix it up ourselves.
     */
    private RabbitConfig parseBindings(final RabbitConfig rabbitConfig) {
        rabbitConfig.getBindings().keySet().forEach(key -> {
            final List<BindingData> bindings = new ArrayList<>();
            ((List) rabbitConfig.getBindings().get(key)).forEach(b -> {
                Map<String, Object> bindingMap = (Map<String, Object>) b;
                bindings.add(new BindingData(
                        bindingMap.get("destination") != null ? bindingMap.get("destination").toString() : null,
                        bindingMap.get("destination_type") != null ? bindingMap.get("destination_type").toString() : null,
                        bindingMap.get("routing_key") != null ? bindingMap.get("routing_key").toString() : null,
                        bindingMap.get("arguments") != null ? (Map<String, Object>) bindingMap.get("arguments") : null));
            });
            rabbitConfig.getBindings().replace(key, bindings);
        });
        return rabbitConfig;
    }
}
