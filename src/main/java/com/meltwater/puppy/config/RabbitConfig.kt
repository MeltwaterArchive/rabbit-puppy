package com.meltwater.puppy.config

import java.util.*

data class User(val name: String, var properties: UserData)
data class Vhost(val name: String, var properties: VHostData)
data class Permissions(val userName: String, var vhost: String, var properties: PermissionsData)
data class Exchange(val name: String, val vhost: String, var properties: ExchangeData)
data class Queue(val name: String, val vhost: String, var properties: QueueData)
data class Binding(val exchange: String, val vhost: String, var properties: BindingData)

data class RabbitConfig(
        var vhosts: MutableMap<String, VHostData> = HashMap(),
        var users: MutableMap<String, UserData> = HashMap(),
        var permissions: MutableMap<String, PermissionsData> = HashMap(),
        var exchanges: MutableMap<String, ExchangeData> = HashMap(),
        var queues: MutableMap<String, QueueData> = HashMap(),
        var bindings: MutableMap<String, MutableList<BindingData>> = HashMap()) {

    fun add(it: User): RabbitConfig {
        users.put(it.name, it.properties)
        return this
    }

    fun add(it: Vhost): RabbitConfig {
        vhosts.put(it.name, it.properties)
        return this
    }

    fun add(it: Permissions): RabbitConfig {
        permissions.put("${it.userName}@${it.vhost}", it.properties)
        return this
    }

    fun add(it: Exchange): RabbitConfig {
        exchanges.put("${it.name}@${it.vhost}", it.properties)
        return this
    }

    fun add(it: Queue): RabbitConfig {
        queues.put("${it.name}@${it.vhost}", it.properties)
        return this
    }

    fun add(it: Binding): RabbitConfig {
        val key = "${it.exchange}@${it.vhost}"
        bindings.getOrPut(key, {arrayListOf()}).add(it.properties)
        return this
    }

    companion object {
        fun <T : Any> initProperties(t: T, init: T.() -> Unit): T {
            t.init()
            return t
        }

        fun user(name: String, init: UserData.() -> Unit) =
                User(name, initProperties(UserData(), init))

        fun vhost(name: String, init: VHostData.() -> Unit) =
                Vhost(name, initProperties(VHostData(), init))

        fun permissions(user: String, vhost: String, init: PermissionsData.() -> Unit) =
                Permissions(user, vhost, initProperties(PermissionsData(), init))

        fun exchange(name: String, vhost: String, init: ExchangeData.() -> Unit) =
                Exchange(name, vhost, initProperties(ExchangeData(), init))

        fun queue(name: String, vhost: String, init: QueueData.() -> Unit) =
                Queue(name, vhost, initProperties(QueueData(), init))

        fun binding(exchange: String, vhost: String, init: BindingData.() -> Unit) =
                Binding(exchange, vhost, initProperties(BindingData(), init))
    }
}
