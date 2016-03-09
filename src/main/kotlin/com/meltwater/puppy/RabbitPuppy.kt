package com.meltwater.puppy

import com.meltwater.puppy.action.EnsurePresentAction
import com.meltwater.puppy.action.RabbitAction
import com.meltwater.puppy.action.VerifyConfigAction
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

    @Throws(RabbitPuppyException::class)
    fun apply(config: RabbitConfig) {
        val errors = ArrayList<Throwable>()
        val action = EnsurePresentAction(client);

        if (config.vhosts.size > 0)
            vhosts(action, config.vhosts, errors)

        if (config.users.size > 0)
            users(action, config.users, errors)

        if (config.permissions.size > 0)
            permissions(action, config.permissions, errors)

        if (config.exchanges.size > 0)
            exchanges(action, config, errors)

        if (config.queues.size > 0)
            queues(action, config, errors)

        if (config.bindings.size > 0)
            bindings(action, config, errors)

        if (errors.size > 0) {
            throw RabbitPuppyException("Encountered errors while applying configuration", errors)
        }
    }

    @Throws(RabbitPuppyException::class)
    fun verify(config: RabbitConfig) {
        val errors = ArrayList<Throwable>()
        val action = VerifyConfigAction();

        if (config.vhosts.size > 0)
            vhosts(action, config.vhosts, errors)

        if (config.users.size > 0)
            users(action, config.users, errors)

        if (config.permissions.size > 0)
            permissions(action, config.permissions, errors)

        if (config.exchanges.size > 0)
            exchanges(action, config, errors)

        if (config.queues.size > 0)
            queues(action, config, errors)

        if (config.bindings.size > 0)
            bindings(action, config, errors)

        if (errors.size > 0) {
            throw RabbitPuppyException("Encountered errors while applying configuration", errors)
        }
    }

    private fun vhosts(action: RabbitAction, vhosts: Map<String, VHostData>, errors: MutableList<Throwable>) {
        try {
            val existing: Map<String, VHostData> = client.getVirtualHosts()
            vhosts.entries.forEach { entry ->
                try {
                    action.vhost(entry.key, entry.value, existing);
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
        } catch (e: RestClientException) {
            log.error("Failed to fetch vhosts", e)
            errors.add(e)
        }

    }

    private fun users(action: RabbitAction, users: Map<String, UserData>, errors: MutableList<Throwable>) {
        try {
            val existing = withKnownPasswords(client.getUsers(), users)
            users.entries.forEach { entry ->
                try {
                    action.user(entry.key, entry.value, existing)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
        } catch (e: RestClientException) {
            log.error("Failed to fetch vhosts", e)
            errors.add(e)
        }

    }

    private fun permissions(action: RabbitAction, permissions: Map<String, PermissionsData>, errors: MutableList<Throwable>) {
        try {
            val existing = client.getPermissions()
            permissions.entries.forEach { entry ->
                val name = entry.key
                val matcher = atVHostPattern.matcher(name)
                if (matcher.matches()) {
                    try {
                        action.permissions(matcher.group(1), matcher.group(2), entry.value, existing);
                    } catch (e: Exception) {
                        errors.add(e)
                    }
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

    private fun exchanges(action: RabbitAction, config: RabbitConfig, errors: MutableList<Throwable>) {
        config.exchanges.entries.forEach { entry ->
            val name = entry.key
            val matcher = atVHostPattern.matcher(name)
            if (matcher.matches()) {
                val exchange = matcher.group(1)
                val vhost = matcher.group(2)
                val data = entry.value
                val auth = authForResource(config.users, config.permissions, vhost, exchange)
                log.debug("Ensuring exchange $exchange exists at vhost $vhost with configuration $data")
                try {
                    val existing = client.getExchange(vhost, exchange, auth.first, auth.second)
                    action.exchange(exchange, vhost, data, existing, auth)
                } catch (e: Exception) {
                    errors.add(e)
                }

            } else {
                val error = "Invalid exchange format '$name', should be exchange@vhost"
                log.error(error)
                errors.add(RabbitConfigException(error))
            }
        }
    }

    private fun queues(action: RabbitAction, config: RabbitConfig, errors: MutableList<Throwable>) {
        config.queues.entries.forEach { entry ->
            val name = entry.key
            val matcher = atVHostPattern.matcher(name)
            if (matcher.matches()) {
                val queue = matcher.group(1)
                val vhost = matcher.group(2)
                val data = entry.value
                val auth = authForResource(config.users, config.permissions, vhost, queue)
                try {
                    val existing = client.getQueue(vhost, queue, auth.first, auth.second)
                    action.queue(queue, vhost, data, existing, auth)
                } catch (e: Exception) {
                    errors.add(e)
                }

            } else {
                val error = "Invalid queue format '$name', should be queue@vhost"
                log.error(error)
                errors.add(RabbitConfigException(error))
            }
        }
    }

    private fun bindings(action: RabbitAction, config: RabbitConfig, errors: MutableList<Throwable>) {
        config.bindings.entries.forEach { entry ->
            val name = entry.key
            val matcher = atVHostPattern.matcher(name)
            if (matcher.matches()) {
                val exchange = matcher.group(1)
                val vhost = matcher.group(2)
                val auth = authForResource(config.users, config.permissions, vhost, exchange)
                try {
                    val existingVhost = client.getBindings(vhost, auth.first, auth.second)
                    entry.value.forEach { binding ->
                        try {
                            action.binding(exchange, vhost, binding, existingVhost, auth)
                        } catch (e: Exception) {
                            errors.add(e)
                        }
                    }
                } catch (e: Exception) {
                    log.error("Failed to fetch existing bindings from $name", e)
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
     * Copies known passwords onto users received from broker, since we get only password hash from it, so that
     * ensurePresent does not fail due to differences in the password field.

     * @param existing   Existing users
     * @param fromConfig Users from input configuration
     * @return Existing users with known passwords appended
     */
    private fun withKnownPasswords(existing: Map<String, UserData>,
                                   fromConfig: Map<String, UserData>): Map<String, UserData> {
        existing.forEach { entry ->
            entry.value.password = fromConfig.getOrElse(entry.key, {UserData()}).password
        }
        return existing
    }

    /**
     * Attempts to find a user in the given configuration with rights to configure the requested resource.
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
