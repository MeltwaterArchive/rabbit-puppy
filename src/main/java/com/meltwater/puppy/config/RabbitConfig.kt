package com.meltwater.puppy.config

import java.util.*

data class RabbitConfig(
        var vhosts: MutableMap<String, VHostData> = HashMap(),
        var users: MutableMap<String, UserData> = HashMap(),
        var permissions: MutableMap<String, PermissionsData> = HashMap(),
        var exchanges: MutableMap<String, ExchangeData> = HashMap(),
        var queues: MutableMap<String, QueueData> = HashMap(),
        var bindings: MutableMap<String, MutableList<BindingData>> = HashMap()) {

    fun <T : Any> initProperties(t: T, init: T.() -> Unit): T {
        t.init()
        return t
    }

    fun addUser(name: String, init: UserData.() -> Unit): RabbitConfig {
        users.put(name, initProperties(UserData(), init))
        return this
    }

    fun addVhost(name: String, init: VHostData.() -> Unit): RabbitConfig {
        vhosts.put(name, initProperties(VHostData(), init))
        return this
    }

    fun addPermissions(user: String, vhost: String, init: PermissionsData.() -> Unit): RabbitConfig {
        permissions.put("$user@$vhost", initProperties(PermissionsData(), init))
        return this
    }

    fun addExchange(name: String, vhost: String, init: ExchangeData.() -> Unit): RabbitConfig {
        exchanges.put("$name@$vhost", initProperties(ExchangeData(), init))
        return this
    }

    fun addQueue(name: String, vhost: String, init: QueueData.() -> Unit): RabbitConfig {
        queues.put("$name@$vhost", initProperties(QueueData(), init))
        return this
    }

    fun addBinding(exchange: String, vhost: String, init: BindingData.() -> Unit): RabbitConfig {
        val key = "$exchange@$vhost"
        bindings.getOrPut(key, {arrayListOf()}).add(initProperties(BindingData(), init))
        return this
    }
}
