package com.meltwater.puppy.config

enum class ExchangeType { MISSING, direct, topic, fanout, headers }

data class ExchangeData(var type: ExchangeType = ExchangeType.MISSING,
                        var durable: Boolean = true,
                        var auto_delete: Boolean = false,
                        var internal: Boolean = false,
                        var arguments: MutableMap<String, Any> = java.util.HashMap()) {

    fun addArgument(key: String, value: Any): ExchangeData {
        arguments.put(key, value)
        return this
    }
}
