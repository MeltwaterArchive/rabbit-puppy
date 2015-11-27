package com.meltwater.puppy;

import com.meltwater.puppy.config.BindingData;
import com.meltwater.puppy.config.ExchangeData;
import com.meltwater.puppy.config.PermissionsData;
import com.meltwater.puppy.config.QueueData;
import com.meltwater.puppy.config.RabbitConfig;
import com.meltwater.puppy.config.UserData;
import com.meltwater.puppy.config.VHostData;
import com.meltwater.puppy.config.reader.RabbitConfigException;
import com.meltwater.puppy.rest.RabbitRestClient;
import com.meltwater.puppy.rest.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class RabbitPuppy {

    private static final Logger log = LoggerFactory.getLogger(RabbitPuppy.class);

    private final Pattern atVHostPattern = Pattern.compile("([^@]+)@([^@]+)");

    private final RabbitRestClient client;

    public RabbitPuppy(String brokerAddress, String username, String password) {
        client = new RabbitRestClient(brokerAddress, username, password);
    }

    public RabbitPuppy(RabbitRestClient client) {
        this.client = client;
    }

    public boolean waitForBroker(int seconds) {
        long start = new Date().getTime();
        while (new Date().getTime() < start + seconds*1000) {
            if (client.ping()) {
                return true;
            } else {
                log.info("Broker not available, waiting...");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        return false;
    }

    /**
     * Apply configuration to RabbitMQ Broker
     *
     * @param config Configuration to apply
     * @throws RabbitPuppyException If errors or configuration mismatches are encountered
     */
    public void apply(RabbitConfig config) throws RabbitPuppyException {
        List<Throwable> errors = new ArrayList<>();

        if (config.getVhosts().size() > 0)
            createVHosts(config.getVhosts(), errors);

        if (config.getUsers().size() > 0)
            createUsers(config.getUsers(), errors);

        if (config.getPermissions().size() > 0)
            createPermissions(config.getPermissions(), errors);

        if (config.getExchanges().size() > 0)
            createExchanges(config, errors);

        if (config.getQueues().size() > 0)
            createQueues(config, errors);

        if (config.getBindings().size() > 0)
            createBindings(config, errors);

        if (errors.size() > 0) {
            throw new RabbitPuppyException("Encountered errors while applying configuration", errors);
        }
    }

    /**
     * Create vhosts based on configuration.
     *
     * @param vhosts Configured vhosts
     * @return List of errors encountered during creation
     */
    private void createVHosts(Map<String, VHostData> vhosts, final List<Throwable> errors) {
        try {
            final Map<String, VHostData> existing = client.getVirtualHosts();
            vhosts.entrySet().forEach(entry -> {
                String name = entry.getKey();
                VHostData data = entry.getValue() == null ? new VHostData() : entry.getValue();
                ensurePresent("vhost", name, data, existing, errors, () -> {
                    log.info("Creating vhost " + entry.getKey());
                    client.createVirtualHost(entry.getKey(), data);
                });
            });
        } catch (RestClientException e) {
            log.error("Failed to fetch vhosts", e);
            errors.add(e);
        }
    }

    /**
     * Create users based on configuration.
     *
     * @param users Configured users
     * @return List of errors encountered during creation
     */
    private void createUsers(Map<String, UserData> users, final List<Throwable> errors) {
        try {
            final Map<String, UserData> existing = withKnownPasswords(client.getUsers(), users);
            users.entrySet().forEach(entry -> {
                String name = entry.getKey();
                UserData data = entry.getValue() == null ? new UserData() : entry.getValue();
                ensurePresent("user", name, data, existing, errors, () -> {
                    log.info("Creating user " + entry.getKey());
                    client.createUser(entry.getKey(), data);
                });
            });
        } catch (RestClientException e) {
            log.error("Failed to fetch vhosts", e);
            errors.add(e);
        }
    }

    /**
     * Creates user permissions per vhost based on configuration.
     *
     * @param permissions Configured permissions
     * @return List of errors encountered during creation
     */
    private void createPermissions(Map<String, PermissionsData> permissions, final List<Throwable> errors) {
        try {
            final Map<String, PermissionsData> existing = client.getPermissions();
            permissions.entrySet().forEach(entry -> {
                String name = entry.getKey();
                final Matcher matcher = atVHostPattern.matcher(name);
                if (matcher.matches()) {
                    PermissionsData data = entry.getValue() == null ? new PermissionsData() : entry.getValue();
                    ensurePresent("permissions", name, data, existing, errors, () -> {
                        String user = matcher.group(1);
                        String vhost = matcher.group(2);
                        log.info(format("Setting permissions for user %s at vhost %s", user, vhost));
                        client.createPermissions(user, vhost, data);
                    });
                } else {
                    String error = format("Invalid exchange format '%s', should be exchange@vhost", name);
                    log.error(error);
                    errors.add(new RabbitConfigException(error));
                }
            });
        } catch (RestClientException e) {
            log.error("Failed to fetch exchanges", e);
            errors.add(e);
        }
    }

    /**
     * Creates exchanges based on configuration.
     *
     * @param config Rabbit configuration
     * @return List of errors encountered during creation
     */
    private void createExchanges(RabbitConfig config, final List<Throwable> errors) {
        config.getExchanges().entrySet().forEach(entry -> {
            String name = entry.getKey();
            final Matcher matcher = atVHostPattern.matcher(name);
            if (matcher.matches()) {
                final String exchange = matcher.group(1);
                final String vhost = matcher.group(2);
                ExchangeData data = entry.getValue() == null ? new ExchangeData() : entry.getValue();
                log.debug(format("Ensuring exchange %s exists at vhost %s with configuration %s", exchange, vhost, data));
                final Auth auth = authForResource(config.getUsers(), config.getPermissions(), vhost, exchange);
                try {
                    Optional<ExchangeData> existing = client.getExchange(vhost, exchange, auth.getUser(), auth.getPass());
                    ensurePresent("exchange", name, data, existing, errors, () -> {
                        log.info(format("Creating exchange %s at vhost %s with configuration %s", exchange, vhost, data));
                        client.createExchange(vhost, exchange, data, auth.getUser(), auth.getPass());
                    });
                } catch (RestClientException e) {
                    log.error(format("Exception when ensuring exchange %s", exchange), e);
                    errors.add(e);
                }
            } else {
                String error = format("Invalid exchange format '%s', should be exchange@vhost", name);
                log.error(error);
                errors.add(new RabbitConfigException(error));
            }
        });
    }

    /**
     * Creates queues based on configuration.
     *
     * @param config Rabbit configuration
     * @return List of errors encountered during creation
     */
    private void createQueues(RabbitConfig config, final List<Throwable> errors) {
        config.getQueues().entrySet().forEach(entry -> {
            String name = entry.getKey();
            final Matcher matcher = atVHostPattern.matcher(name);
            if (matcher.matches()) {
                final String queue = matcher.group(1);
                final String vhost = matcher.group(2);
                QueueData data = entry.getValue() == null ? new QueueData() : entry.getValue();
                log.debug(format("Ensuring queue %s exists at vhost %s with configuration %s", queue, vhost, data));
                final Auth auth = authForResource(config.getUsers(), config.getPermissions(), vhost, queue);
                try {
                    Optional<QueueData> existing = client.getQueue(vhost, queue, auth.getUser(), auth.getPass());
                    ensurePresent("queue", name, data, existing, errors, () -> {
                        log.info(format("Creating queue %s at vhost %s with configuration %s", queue, vhost, data));
                        client.createQueue(vhost, queue, data, auth.getUser(), auth.getPass());
                    });
                } catch (RestClientException e) {
                    log.error(format("Exception when ensuring queue %s", queue), e);
                    errors.add(e);
                }
            } else {
                String error = format("Invalid queue format '%s', should be queue@vhost", name);
                log.error(error);
                errors.add(new RabbitConfigException(error));
            }
        });
    }

    private void createBindings(RabbitConfig config, final List<Throwable> errors) {
        config.getBindings().entrySet().forEach(entry -> {
            final String name = entry.getKey();
            final Matcher matcher = atVHostPattern.matcher(name);
            if (matcher.matches()) {
                final String exchange = matcher.group(1);
                final String vhost = matcher.group(2);
                final List<BindingData> bindings = (List<BindingData>) entry.getValue();
                final Auth auth = authForResource(config.getUsers(), config.getPermissions(), vhost, exchange);
                try {
                    final Map<String, List<BindingData>> existingVhost = client.getBindings(vhost, auth.user, auth.pass);
                    final List<BindingData> existing = existingVhost.containsKey(exchange) ?
                            existingVhost.get(exchange) :
                            new ArrayList<>();
                    bindings.forEach(bindingData -> {
                        log.info(format("Ensuring binding %s %s", name, bindingData));
                        if (!existing.contains(bindingData)) {
                            log.info(format("Creating binding %s : %s", name, bindingData));
                            try {
                                client.createBinding(vhost,
                                                     exchange,
                                                     bindingData,
                                                     auth.user,
                                                     auth.pass);
                            } catch (RestClientException e) {
                                log.error(format("Failed to create binding %s %s: %s", name, bindingData, e.getMessage()));
                                errors.add(e);
                            }
                        }
                    });
                } catch (RestClientException e) {
                    log.error(format("Failed to fetch bindings from %s", name), e);
                    errors.add(e);
                }
            } else {
                String error = format("Invalid binding format '%s', should be exchange@vhost", name);
                log.error(error);
                errors.add(new RabbitConfigException(error));
            }
        });
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private <D> void ensurePresent(String type, String name, D data, Map<String, D> existing, List<Throwable> errors, Create create) {
        if (existing.containsKey(name)) {
            if (!existing.get(name).equals(data)) {
                String error = format("%s '%s' exists but with wrong configuration: %s, expected: %s",
                        type, name, existing.get(name), data);
                log.error(error);
                errors.add(new InvalidConfigurationException(error));
            }
        } else {
            try {
                create.create();
            } catch (RestClientException e) {
                log.error(format("Failed to create %s '%s': %s", type, name, e.getMessage()));
                errors.add(e);
            }
        }
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private <D> void ensurePresent(String type, String name, D data, Optional<D> existing, List<Throwable> errors, Create create) {
        if (existing.isPresent()) {
            if (!existing.get().equals(data)) {
                String error = format("%s '%s' exists but with wrong configuration: %s, expected: %s",
                        type, name, existing.get(), data);
                log.error(error);
                errors.add(new InvalidConfigurationException(error));
            }
        } else {
            try {
                create.create();
            } catch (RestClientException e) {
                log.error(format("Failed to create %s '%s': %s", type, name, e.getMessage()));
                errors.add(e);
            }
        }
    }

    /**
     * Because lambdas.
     */
    private interface Create {
        void create() throws RestClientException;
    }

    /**
     * Copies known passwords onto users received from broker, since we get only password hash from it, so that
     * ensurePresent does not fail due to differences in the password field.
     *
     * @param existing   Existing users
     * @param fromConfig Users from input configuration
     * @return Existing users with known passwords appended
     */
    private Map<String, UserData> withKnownPasswords(Map<String, UserData> existing,
                                                     Map<String, UserData> fromConfig) {
        existing.forEach((user, data) -> {
            if (fromConfig.containsKey(user)) {
                data.setPassword(fromConfig.get(user).getPassword());
            }
        });
        return existing;
    }

    /**
     * Attempts to find a user in the given configuration with rights to configure the requested resource.
     *
     * @param permissions  user permissions
     * @param vhost        vhost name
     * @param resourceName resource name
     * @return Optional of user with creation rights, or Optional.empty() if not found.
     */
    private Auth authForResource(Map<String, UserData> users,
                                 Map<String, PermissionsData> permissions,
                                 String vhost,
                                 String resourceName) {
        Optional<String> found = permissions.entrySet().stream()
                .filter(entry -> {
                    final Matcher matcher = atVHostPattern.matcher(entry.getKey());
                    if (matcher.matches() && matcher.group(2).equals(vhost)) {
                        if (Pattern.compile(entry.getValue().getConfigure()).matcher(resourceName).matches()) {
                            return true;
                        }
                    }
                    return false;
                })
                .map(entry -> {
                    Matcher matcher = atVHostPattern.matcher(entry.getKey());
                    matcher.find(); // Causes matcher to actually parse regex groups
                    return matcher.group(1);
                })
                .findFirst();

        if (found.isPresent()) {
            String user = found.get();
            String pass = users.get(user).getPassword();
            return new Auth(user, pass);
        } else {
            return defaultAuth();
        }
    }

    private Auth defaultAuth() {
        return new Auth(client.getUsername(), client.getPassword());
    }

    private class Auth {
        private final String user;
        private final String pass;

        public Auth(String user, String pass) {
            this.user = user;
            this.pass = pass;
        }

        public String getUser() {
            return user;
        }

        public String getPass() {
            return pass;
        }
    }
}
