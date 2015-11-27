package com.meltwater.puppy.config


data class ExchangeData(var type: String = "",
                        var durable: Boolean = true,
                        var auto_delete: Boolean = false,
                        var internal: Boolean = false,
                        var arguments: kotlin.MutableMap<kotlin.String, kotlin.Any>? = java.util.HashMap()) {

    fun addArgument(key: String, value: Any): ExchangeData {
        arguments!!.put(key, value)
        return this
    }
}
