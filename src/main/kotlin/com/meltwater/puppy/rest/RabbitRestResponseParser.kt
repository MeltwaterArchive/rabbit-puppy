package com.meltwater.puppy.rest

import com.google.gson.Gson
import com.meltwater.puppy.config.*
import java.util.*
import javax.ws.rs.core.Response

class RabbitRestResponseParser {

    private val gson = Gson()

    @Throws(RestClientException::class)
    fun vhosts(response: Response): Map<String, VHostData> {
        try {
            val list = gson.fromJson<List<Any>>(response.readEntity(String::class.java), List::class.java)
            val vhosts = HashMap<String, VHostData>()
            for (i in list.indices) {
                val map = list[i] as Map<Any, Any>
                val name = map["name"] as String
                val vHostData = VHostData(map["tracing"] as Boolean)
                vhosts.put(name, vHostData)
            }
            return vhosts
        } catch (e: Exception) {
            throw RestClientException("Error parsing vhosts response", e)
        }

    }

    @Throws(RestClientException::class)
    fun users(response: Response): Map<String, UserData> {
        try {
            val list = gson.fromJson<List<Any>>(response.readEntity(String::class.java), List::class.java)
            val users = HashMap<String, UserData>()
            for (i in list.indices) {
                val map = list[i] as Map<Any, Any>
                val name = map["name"] as String
                val admin = (map["tags"] as String).contains("administrator")
                val userData = UserData("", admin)
                users.put(name, userData)
            }
            return users
        } catch (e: Exception) {
            throw RestClientException("Error parsing users response", e)
        }

    }

    @Throws(RestClientException::class)
    fun permissions(response: Response): Map<String, PermissionsData> {
        try {
            val list = gson.fromJson<List<Any>>(response.readEntity(String::class.java), List::class.java)
            val permissions = HashMap<String, PermissionsData>()
            for (i in list.indices) {
                val map = list[i] as Map<Any, Any>
                val user = map["user"] as String
                val vhost = map["vhost"] as String
                val permissionsData = PermissionsData(
                        map["configure"] as String,
                        map["write"] as String,
                        map["read"] as String)
                permissions.put(user + "@" + vhost, permissionsData)
            }
            return permissions
        } catch (e: Exception) {
            throw RestClientException("Error parsing permissions response", e)
        }

    }

    @Throws(RestClientException::class)
    fun exchange(response: Optional<Response>): Optional<ExchangeData> {
        if (!response.isPresent) {
            return Optional.empty<ExchangeData>()
        }
        try {
            val str: String = response.get().readEntity(String::class.java)
            val map = gson.fromJson<Map<Any, Any>>(str, Object::class.java) // Object instead of Map to handle duplicate keys
            return Optional.of(ExchangeData(
                    ExchangeType.valueOf(map["type"].toString()),
                    map["durable"] as Boolean,
                    map["auto_delete"] as Boolean,
                    map["internal"] as Boolean,
                    convertArgumentTypes(map["arguments"] as MutableMap<String, Any>)))
        } catch (e: Exception) {
            throw RestClientException("Error parsing exchanges response: $e")
        }

    }

    @Throws(RestClientException::class)
    fun queue(response: Optional<Response>): Optional<QueueData> {
        if (!response.isPresent) {
            return Optional.empty<QueueData>()
        }
        try {
            val str: String = response.get().readEntity(String::class.java)
            val map = gson.fromJson<Map<Any, Any>>(str, Object::class.java) // Object instead of Map to handle duplicate keys
            return Optional.of(QueueData(
                    map["durable"] as Boolean,
                    map["auto_delete"] as Boolean,
                    convertArgumentTypes(map["arguments"] as MutableMap<String, Any>)))
        } catch (e: Exception) {
            throw RestClientException("Error parsing queues response: $e")
        }

    }

    @Throws(RestClientException::class)
    fun bindings(response: Response): Map<String, List<BindingData>> {
        try {
            val list = gson.fromJson<List<Any>>(response.readEntity(String::class.java), List::class.java)
            val bindings = HashMap<String, MutableList<BindingData>>()
            for (i in list.indices) {
                val map = list[i] as Map<Any, Any>
                val exchange = map["source"] as String
                if (!bindings.containsKey(exchange)) {
                    bindings.put(exchange, ArrayList<BindingData>())
                }
                val bindingData = BindingData(
                        map["destination"] as String,
                        DestinationType.valueOf(map["destination_type"] as String),
                        map["routing_key"] as String,
                        convertArgumentTypes(map["arguments"] as MutableMap<String, Any>))
                bindings[exchange]!!.add(bindingData)
            }
            return bindings
        } catch (e: Exception) {
            throw RestClientException("Error parsing bindings response: $e")
        }
    }

    fun convertArgumentTypes(arguments: MutableMap<String, Any>) : MutableMap<String, Any> {
        try {
            for (key in arguments.keys) {
                if (key.equals("x-message-ttl")) {
                    val value: Any? = arguments[key]
                    if (value is Double) {
                        arguments[key] = value.toInt()
                    }
                }
            }
            return arguments
        } catch (e: Exception) {
            throw RestClientException("Error parsing arguments in response: $e")
        }
    }
}
