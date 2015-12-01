package com.meltwater.puppy

import com.meltwater.puppy.config.*
import com.meltwater.puppy.config.reader.RabbitConfigException
import com.meltwater.puppy.rest.RabbitRestClient
import com.meltwater.puppy.rest.RestClientException
import org.slf4j.LoggerFactory
import java.util.*
import java.util.regex.Pattern

class RabbitPuppy {

    private val log = LoggerFactory.getLogger(RabbitPuppy::class.java)

    private val atVHostPattern = Pattern.compile("([^@]+)@([^@]+)")

    private val client: RabbitRestClient

    constructor(brokerAddress: String, username: String, password: String) {
        client = RabbitRestClient(brokerAddress, username, password)
    }

    constructor(client: RabbitRestClient) {
        this.client = client
    }

    fun waitForBroker(seconds: Int): Boolean {
        val start = Date().time
        while (Date().time < start + seconds * 1000) {
            if (client.ping()) {
                return true
            } else {
                log.info("Broker not available, waiting...")
            }
            try {
                Thread.sleep(1000)
            } catch (ignored: InterruptedException) {
            }

        }
        return false
    }

    /**
     * Apply configuration to RabbitMQ Broker

     * @param config Configuration to apply
     * *
     * @throws RabbitPuppyException If errors or configuration mismatches are encountered
     */
    @Throws(RabbitPuppyException::class)
    fun apply(config: RabbitConfig) {
        val errors = ArrayList<Throwable>()

        if (config.vhosts.size > 0)
            createVHosts(config.vhosts, errors)

        if (config.users.size > 0)
            createUsers(config.users, errors)

        if (config.permissions.size > 0)
            createPermissions(config.permissions, errors)

        if (config.exchanges.size > 0)
            createExchanges(config, errors)

        if (config.queues.size > 0)
            createQueues(config, errors)

        if (config.bindings.size > 0)
            createBindings(config, errors)

        if (errors.size > 0) {
            throw RabbitPuppyException("Encountered errors while applying configuration", errors)
        }
    }

    /**
     * Create vhosts based on configuration.

     * @param vhosts Configured vhosts
     * *
     * @return List of errors encountered during creation
     */
    private fun createVHosts(vhosts: Map<String, VHostData>, errors: MutableList<Throwable>) {
        try {
            val existing: Map<String, VHostData> = client.getVirtualHosts()
            vhosts.entries.forEach { entry ->
                val name: String = entry.key
                val data: VHostData = entry.value
                ensurePresent("vhost", name, data, existing, errors, {
                    log.info("Creating vhost $name")
                    client.createVirtualHost(name, data)
                })
            }
        } catch (e: RestClientException) {
            log.error("Failed to fetch vhosts", e)
            errors.add(e)
        }

    }

    /**
     * Create users based on configuration.

     * @param users Configured users
     * *
     * @return List of errors encountered during creation
     */
    private fun createUsers(users: Map<String, UserData>, errors: MutableList<Throwable>) {
        try {
            val existing = withKnownPasswords(client.getUsers(), users)
            users.entries.forEach { entry ->
                val name = entry.key
                val data = entry.value
                ensurePresent("user", name, data, existing, errors, {
                    log.info("Creating user $name")
                    client.createUser(entry.key, data)
                })
            }
        } catch (e: RestClientException) {
            log.error("Failed to fetch vhosts", e)
            errors.add(e)
        }

    }

    /**
     * Creates user permissions per vhost based on configuration.

     * @param permissions Configured permissions
     * *
     * @return List of errors encountered during creation
     */
    private fun createPermissions(permissions: Map<String, PermissionsData>, errors: MutableList<Throwable>) {
        try {
            val existing = client.getPermissions()
            permissions.entries.forEach { entry ->
                val name = entry.key
                val matcher = atVHostPattern.matcher(name)
                if (matcher.matches()) {
                    val data = entry.value
                    ensurePresent<PermissionsData>("permissions", name, data, existing, errors, {
                        val user = matcher.group(1)
                        val vhost = matcher.group(2)
                        log.info("Setting permissions for user $user at vhost $vhost")
                        client.createPermissions(user, vhost, data)
                    })
                } else {
                    val error = "Invalid exchange format '$name', should be exchange@vhost"
                    log.error(error)
                    errors.add(RabbitConfigException(error))
                }
            }
        } catch (e: RestClientException) {
            log.error("Failed to fetch exchanges", e)
            errors.add(e)
        }

    }

    /**
     * Creates exchanges based on configuration.

     * @param config Rabbit configuration
     * *
     * @return List of errors encountered during creation
     */
    private fun createExchanges(config: RabbitConfig, errors: MutableList<Throwable>) {
        config.exchanges.entries.forEach { entry ->
            val name = entry.key
            val matcher = atVHostPattern.matcher(name)
            if (matcher.matches()) {
                val exchange = matcher.group(1)
                val vhost = matcher.group(2)
                val data = entry.value
                log.debug("Ensuring exchange $exchange exists at vhost $vhost with configuration $data")
                val auth = authForResource(config.users, config.permissions, vhost, exchange)
                try {
                    val existing = client.getExchange(vhost, exchange, auth.first, auth.second)
                    ensurePresent<ExchangeData>("exchange", name, data, existing, errors, {
                        log.info("Creating exchange $exchange at vhost $vhost with configuration $data")
                        client.createExchange(vhost, exchange, data, auth.first, auth.second)
                    })
                } catch (e: RestClientException) {
                    log.error("Exception when ensuring exchange $exchange", e)
                    errors.add(e)
                }

            } else {
                val error = "Invalid exchange format '$name', should be exchange@vhost"
                log.error(error)
                errors.add(RabbitConfigException(error))
            }
        }
    }

    /**
     * Creates queues based on configuration.

     * @param config Rabbit configuration
     * *
     * @return List of errors encountered during creation
     */
    private fun createQueues(config: RabbitConfig, errors: MutableList<Throwable>) {
        config.queues.entries.forEach { entry ->
            val name = entry.key
            val matcher = atVHostPattern.matcher(name)
            if (matcher.matches()) {
                val queue = matcher.group(1)
                val vhost = matcher.group(2)
                val data = entry.value
                log.debug("Ensuring queue $queue exists at vhost $vhost with configuration $data")
                val auth = authForResource(config.users, config.permissions, vhost, queue)
                try {
                    val existing = client.getQueue(vhost, queue, auth.first, auth.second)
                    ensurePresent<QueueData>("queue", name, data, existing, errors, {
                        log.info("Creating queue $queue at vhost $vhost with configuration $data")
                        client.createQueue(vhost, queue, data, auth.first, auth.second)
                    })
                } catch (e: RestClientException) {
                    log.error("Exception when ensuring queue $queue", e)
                    errors.add(e)
                }

            } else {
                val error = "Invalid queue format '$name', should be queue@vhost"
                log.error(error)
                errors.add(RabbitConfigException(error))
            }
        }
    }

    private fun createBindings(config: RabbitConfig, errors: MutableList<Throwable>) {
        config.bindings.entries.forEach { entry ->
            val name = entry.key
            val matcher = atVHostPattern.matcher(name)
            if (matcher.matches()) {
                val exchange = matcher.group(1)
                val vhost = matcher.group(2)
                val auth = authForResource(config.users, config.permissions, vhost, exchange)
                try {
                    val existingVhost = client.getBindings(vhost, auth.first, auth.second)
                    val existing = existingVhost.getOrElse(exchange, {ArrayList<BindingData>()})
                    entry.value.forEach { bindingData ->
                        log.info("Ensuring binding $name $bindingData")
                        if (!existing.contains(bindingData)) {
                            log.info("Creating binding $name : $bindingData")
                            try {
                                client.createBinding(vhost, exchange, bindingData, auth.first, auth.second)
                            } catch (e: RestClientException) {
                                log.error("Failed to create binding $name $bindingData: ${e.message}")
                                errors.add(e)
                            }

                        }
                    }
                } catch (e: RestClientException) {
                    log.error("Failed to fetch bindings from $name", e)
                    errors.add(e)
                }

            } else {
                val error = "Invalid binding format '$name', should be exchange@vhost"
                log.error(error)
                errors.add(RabbitConfigException(error))
            }
        }
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private fun <D> ensurePresent(type: String, name: String, data: D, existing: Map<String, D>, errors: MutableList<Throwable>, create: () -> Unit) {
        if (existing.containsKey(name)) {
            if (existing[name] != data) {
                val error = "$type '$name' exists but with wrong configuration: $existing, expected: $data"
                log.error(error)
                errors.add(InvalidConfigurationException(error))
            }
        } else {
            try {
                create()
            } catch (e: RestClientException) {
                log.error("Failed to create $type '$name': ${e.message}".format(type, name, e.message))
                errors.add(e)
            }

        }
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private fun <D> ensurePresent(type: String, name: String, data: D, existing: Optional<D>, errors: MutableList<Throwable>, create: () -> Unit) {
        if (existing.isPresent) {
            if (existing.get() != data) {
                val error = "$type '$name' exists but with wrong configuration: ${existing.get()}, expected: $data"
                log.error(error)
                errors.add(InvalidConfigurationException(error))
            }
        } else {
            try {
                create()
            } catch (e: RestClientException) {
                log.error("Failed to create $type '$name': ${e.message}")
                errors.add(e)
            }

        }
    }

    /**
     * Copies known passwords onto users received from broker, since we get only password hash from it, so that
     * ensurePresent does not fail due to differences in the password field.

     * @param existing   Existing users
     * *
     * @param fromConfig Users from input configuration
     * *
     * @return Existing users with known passwords appended
     */
    private fun withKnownPasswords(existing: Map<String, UserData>,
                                   fromConfig: Map<String, UserData>): Map<String, UserData> {
        existing.forEach { entry ->
            entry.value.password = fromConfig.getOrElse(entry.key, {""}).toString()
        }
        return existing
    }

    /**
     * Attempts to find a user in the given configuration with rights to configure the requested resource.

     * @param permissions  user permissions
     * *
     * @param vhost        vhost name
     * *
     * @param resourceName resource name
     * *
     * @return Optional of user with creation rights, or Optional.empty() if not found.
     */
    private fun authForResource(users: Map<String, UserData>,
                                permissions: Map<String, PermissionsData>,
                                vhost: String,
                                resourceName: String): Pair<String, String> {

        val auth = permissions.entries.filter({ entry ->
            // Find permission with rights to edit resourceName on vhost
            var res = false
            val matcher = atVHostPattern.matcher(entry.key)
            if (matcher.matches() && matcher.group(2) == vhost) {
                if (Pattern.compile(entry.value.configure).matcher(resourceName).matches()) {
                    res = true
                }
            }
            res
        }).map({ entry ->
            // Extract user name from permission
            val matcher = atVHostPattern.matcher(entry.key)
            matcher.find() // Causes matcher to actually parse regex groups
            matcher.group(1)
        }).filter({
            // Filter away user if it isn't specified in configuration
            user -> users.contains(user)
        }).map({ user ->
            Pair(user, users[user]!!.password)
        }).firstOrNull()

        if (auth != null) {
            return auth
        } else {
            return defaultAuth()
        }
    }

    private fun defaultAuth(): Pair<String, String> = Pair(client.getUsername(), client.getPassword())
}
