package com.meltwater.puppy.config

import java.util.HashMap

data class BindingData(var destination: String = "",
                       var destination_type: String = "",
                       var routing_key: String = "",
                       var arguments: Map<String, Any>? = HashMap())

