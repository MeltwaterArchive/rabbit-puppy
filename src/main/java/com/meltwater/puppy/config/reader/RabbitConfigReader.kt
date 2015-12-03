package com.meltwater.puppy.config.reader

import com.google.common.base.Joiner
import com.meltwater.puppy.config.BindingData
import com.meltwater.puppy.config.RabbitConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.constructor.ConstructorException

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

class RabbitConfigException : Exception {
    constructor(s: String, e: Exception) : super(s, e)
    constructor(error: String) : super(error)
}

class RabbitConfigReader {

    private val log = LoggerFactory.getLogger(RabbitConfigReader::class.java)

    @Throws(RabbitConfigException::class)
    fun read(yaml: String): RabbitConfig {
        try {
            return parseBindings(Yaml(Constructor(RabbitConfig::class.java)).loadAs(yaml, RabbitConfig::class.java))
        } catch (e: ConstructorException) {
            log.error("Failed reading configuration: ${e.message}")
            throw RabbitConfigException("Failed reading configuration", e)
        }

    }

    @Throws(RabbitConfigException::class)
    fun read(yamlFile: File): RabbitConfig {
        try {
            val lines = Files.readAllLines(Paths.get(yamlFile.absolutePath))
            return read(Joiner.on('\n').join(lines))
        } catch (e: IOException) {
            log.error("Failed reading from file ${yamlFile.path}", e.message)
            throw RabbitConfigException("Failed reading from file " + yamlFile.path, e)
        }

    }

    /**
     * SnakeYAML fails to figure out Bindings types correctly, so we need to fix it up ourselves.
     */
    private fun parseBindings(rabbitConfig: RabbitConfig): RabbitConfig {
        rabbitConfig.bindings.keys.forEach { key ->
            val bindings = ArrayList<BindingData>()
            (rabbitConfig.bindings[key] as List<Any>).forEach { b ->
                val bindingMap = b as Map<String, Any>
                bindings.add(BindingData(
                        if (bindingMap["destination"] != null) bindingMap["destination"].toString() else null,
                        if (bindingMap["destination_type"] != null) bindingMap["destination_type"].toString() else null,
                        if (bindingMap["routing_key"] != null) bindingMap["routing_key"].toString() else null,
                        if (bindingMap.contains("arguments")) bindingMap["arguments"] as MutableMap<String, Any> else HashMap()))
            }
            rabbitConfig.bindings.put(key, bindings)
        }
        return rabbitConfig
    }
}
