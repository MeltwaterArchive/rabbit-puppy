package com.meltwater.puppy.action;

import com.meltwater.puppy.InvalidConfigurationException
import com.meltwater.puppy.config.*
import com.meltwater.puppy.rest.RabbitRestClient
import com.meltwater.puppy.rest.RestClientException
import org.slf4j.LoggerFactory
import java.util.*

class EnsurePresentAction(val client: RabbitRestClient) : RabbitAction {

    private val log = LoggerFactory.getLogger(EnsurePresentAction::class.java)

    override fun vhost(name: String, data: VHostData, existing: Map<String, VHostData>) {
        log.info("Ensuring vhost $name exists with configuration $data")
        ensurePresent("vhost", name, data, existing) {
            log.info("Creating vhost $name")
            client.createVirtualHost(name, data)
        }
    }

    override fun user(name: String, data: UserData, existing: Map<String, UserData>) {
        log.info("Ensuring user $name exists")
        ensurePresent("user", name, data, existing) {
            log.info("Creating user $name")
            client.createUser(name, data)
        }
    }

    override fun permissions(user: String, vhost: String, data: PermissionsData, existing: Map<String, PermissionsData>) {
        log.info("Ensuring user $user at vhost $vhost has permissions $data")
        ensurePresent("permissions", "$user@$vhost", data, existing) {
            log.info("Setting permissions for user $user at vhost $vhost")
            client.createPermissions(user, vhost, data)
        }
    }

    override fun exchange(exchange: String, vhost: String, data: ExchangeData, existing: Optional<ExchangeData>,
                          auth: Pair<String, String>) {
        log.info("Ensuring exchange $exchange exists at vhost $vhost with configuration $data")
        ensurePresent("exchange", "$exchange@$vhost", data, existing) {
            log.info("Creating exchange $exchange at vhost $vhost with configuration $data")
            client.createExchange(vhost, exchange, data, auth.first, auth.second)
        }
    }

    override fun queue(queue: String, vhost: String, data: QueueData, existing: Optional<QueueData>,
                       auth: Pair<String, String>) {
        log.info("Ensuring queue $queue exists at vhost $vhost with configuration $data")
        ensurePresent("queue", "$queue@$vhost", data, existing) {
            log.info("Creating exchange $queue at vhost $vhost with configuration $data")
            client.createQueue(vhost, queue, data, auth.first, auth.second)
        }
    }

    override fun binding(exchange: String, vhost: String, data: BindingData, existing: Map<String, List<BindingData>>,
                         auth: Pair<String, String>) {
        val existingExchange = existing.getOrElse(exchange, { ArrayList<BindingData>() })
        var name = "$exchange@$vhost"
        log.info("Ensuring exchange $name has binding $data")
        if (!existingExchange.contains(data)) {
            log.info("Creating binding $name : $data")
            client.createBinding(vhost, exchange, data, auth.first, auth.second)
        }
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private fun <D> ensurePresent(type: String, name: String, data: D, existing: Map<String, D>, create: () -> Unit) {
        if (existing.containsKey(name)) {
            if (existing[name] != data) {
                val error = "$type '$name' exists but with wrong configuration: $existing, expected: $data"
                log.error(error)
                throw InvalidConfigurationException(error)
            }
        } else {
            try {
                create()
            } catch (e: RestClientException) {
                log.error("Failed to create $type '$name': ${e.message}".format(type, name, e.message))
                throw e
            }

        }
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private fun <D> ensurePresent(type: String, name: String, data: D, existing: Optional<D>, create: () -> Unit) {
        if (existing.isPresent) {
            if (existing.get() != data) {
                val error = "$type '$name' exists but with wrong configuration: ${existing.get()}, expected: $data"
                log.error(error)
                throw InvalidConfigurationException(error)
            }
        } else {
            try {
                create()
            } catch (e: RestClientException) {
                log.error("Failed to create $type '$name': ${e.message}")
                throw e
            }
        }
    }
}
