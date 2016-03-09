package com.meltwater.puppy.action

import com.meltwater.puppy.config.*
import java.util.*

interface RabbitAction {

    fun vhost(name: String, data: VHostData, existing: Map<String, VHostData>)

    fun user(name: String, data: UserData, existing: Map<String, UserData>)

    fun permissions(user: String, vhost: String, data: PermissionsData, existing: Map<String, PermissionsData>)

    fun exchange(exchange: String, vhost: String, data: ExchangeData, existing: Optional<ExchangeData>,
                 auth: Pair<String, String>)

    fun queue(queue: String, vhost: String, data: QueueData, existing: Optional<QueueData>,
              auth: Pair<String, String>)

    fun binding(exchange: String, vhost: String, data: BindingData, existing: Map<String, List<BindingData>>,
                auth: Pair<String, String>)
}