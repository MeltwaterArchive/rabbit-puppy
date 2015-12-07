package com.meltwater.puppy.config

import java.util.*

data class RabbitConfig(
        var vhosts: MutableMap<String, VHostData> = HashMap(),
        var users: MutableMap<String, UserData> = HashMap(),
        var permissions: MutableMap<String, PermissionsData> = HashMap(),
        var exchanges: MutableMap<String, ExchangeData> = HashMap(),
        var queues: MutableMap<String, QueueData> = HashMap(),
        var bindings: MutableMap<String, MutableList<BindingData>> = HashMap()) {

    fun addUser(name: String, data: UserData): RabbitConfig {
        users.put(name, data)
        return this
    }

    fun addVhost(name: String, data: VHostData): RabbitConfig {
        vhosts.put(name, data)
        return this
    }

    fun addPermissions(user: String, vhost: String, data: PermissionsData): RabbitConfig {
        permissions.put("$user@$vhost", data)
        return this
    }

    fun addExchange(name: String, vhost: String, data: ExchangeData): RabbitConfig {
        exchanges.put("$name@$vhost", data)
        return this
    }

    fun addQueue(name: String, vhost: String, data: QueueData): RabbitConfig {
        queues.put("$name@$vhost", data)
        return this
    }

    fun addBinding(exchange: String, vhost: String, data: BindingData): RabbitConfig {
        val key = "$exchange@$vhost"
        bindings.getOrPut(key, {arrayListOf()}).add(data)
        return this
    }
}
