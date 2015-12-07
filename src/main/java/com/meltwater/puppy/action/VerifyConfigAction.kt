package com.meltwater.puppy.action

import com.meltwater.puppy.InvalidConfigurationException
import com.meltwater.puppy.config.*
import org.slf4j.LoggerFactory
import java.util.*

class VerifyConfigAction() : RabbitAction {

    private val log = LoggerFactory.getLogger(VerifyConfigAction::class.java)

    override fun vhost(name: String, data: VHostData, existing: Map<String, VHostData>) {
        log.info("Verifying that vhost $name exists with configuration $data")
        ensurePresent("vhost", name, data, existing,
                "Vhost $name is missing, will be created on apply")
    }

    override fun user(name: String, data: UserData, existing: Map<String, UserData>) {
        log.info("Verifying that user $name exists with configuration ${UserData("", data.admin)}")
        ensurePresent("user", name, data, existing,
                "User $name is missing, will be created on apply")
    }

    override fun permissions(user: String, vhost: String, data: PermissionsData, existing: Map<String, PermissionsData>) {
        log.info("Verifying that user $user on vhost $vhost has permissions $data")
        ensurePresent("permissions", "$user@$vhost", data, existing,
                "Permisssions for user $user at vhost $vhost are missing, will be created on apply")
    }

    override fun exchange(exchange: String, vhost: String, data: ExchangeData, existing: Optional<ExchangeData>,
                          auth: Pair<String, String>) {
        log.info("Verifying that exchange $exchange exists on vhost $vhost with configuration $data")
        ensurePresent("exchange", "$exchange@$vhost", data, existing,
                "Exchange $exchange at vhost $vhost is missing, will be created on apply")
    }

    override fun queue(queue: String, vhost: String, data: QueueData, existing: Optional<QueueData>,
                       auth: Pair<String, String>) {
        log.info("Verifying that queue $queue exists on vhost $vhost with configuration $data")
        ensurePresent("exchange", "$queue@$vhost", data, existing,
                "Queue $queue at vhost $vhost is missing, will be created on apply")
    }

    override fun binding(exchange: String, vhost: String, data: BindingData, existing: Map<String, List<BindingData>>,
                         auth: Pair<String, String>) {
        val existingExchange = existing.getOrElse(exchange, { ArrayList<BindingData>() })
        var name = "$exchange@$vhost"
        log.info("Verifying that exchange $name has binding $data")
        if (!existingExchange.contains(data)) {
            log.error("Exchange $name is missing binding $data, will be created on apply")
        }
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private fun <D> ensurePresent(type: String, name: String, data: D, existing: Map<String, D>, logMissing: String) {
        if (existing.containsKey(name)) {
            if (existing[name] != data) {
                val error = "$type '$name' exists but with wrong configuration: $existing, expected: $data"
                log.error(error)
                throw InvalidConfigurationException(error)
            }
        } else {
            log.error(logMissing)
        }
    }

    /**
     * Ensures that the configured resource is present on the broker.
     * Throws exception if creation failed, or resource exists with settings that does not match expected configuration.
     */
    private fun <D> ensurePresent(type: String, name: String, data: D, existing: Optional<D>, logMissing: String) {
        if (existing.isPresent) {
            if (existing.get() != data) {
                val error = "$type '$name' exists but with wrong configuration: ${existing.get()}, expected: $data"
                log.error(error)
                throw InvalidConfigurationException(error)
            }
        } else {
            log.error(logMissing)
        }
    }
}
