package com.meltwater.puppy.config.reader

import com.google.common.base.Joiner
import com.meltwater.puppy.config.BindingData
import com.meltwater.puppy.config.DestinationType
import com.meltwater.puppy.config.RabbitConfig
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
                val destType = bindingMap["destination_type"]
                try {
                    val destTypeEnum =
                            if (destType != null) DestinationType.valueOf(destType.toString())
                            else DestinationType.MISSING

                    bindings.add(BindingData(
                            bindingMap["destination"]?.toString(),
                            destTypeEnum,
                            bindingMap["routing_key"]?.toString(),
                            bindingMap.getOrElse("arguments", {HashMap<String, Any>()}) as MutableMap<String, Any>))
                }
                catch (e: IllegalArgumentException) {
                    val error = "Invalid destination_type: $destType, must be one of: ${Joiner.on(',').join(
                            DestinationType.values.copyOfRange(1, DestinationType.values.size))}"
                    log.error(error)
                    throw RabbitConfigException(error)
                }
            }
            rabbitConfig.bindings.put(key, bindings)
        }
        return rabbitConfig
    }
}
