package com.meltwater.puppy

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.ParameterException
import com.meltwater.puppy.config.BindingData
import com.meltwater.puppy.config.ExchangeData
import com.meltwater.puppy.config.PermissionsData
import com.meltwater.puppy.config.QueueData
import com.meltwater.puppy.config.RabbitConfig
import com.meltwater.puppy.config.UserData
import com.meltwater.puppy.config.VHostData
import com.meltwater.puppy.config.reader.RabbitConfigException
import com.meltwater.puppy.config.reader.RabbitConfigReader
import com.meltwater.puppy.rest.RabbitRestClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.io.IOException
import java.util.HashMap

class RabbitPuppyException(s: String, val errors: List<Throwable>) : Exception(s)

private val log = LoggerFactory.getLogger("Main")

private val rabbitConfigReader = RabbitConfigReader()

private class Arguments {
    @Parameter(names = arrayOf("-h", "--help"), description = "Print help and exit", help = true)
    public var help: Boolean = false

    @Parameter(names = arrayOf("-c", "--config"), description = "YAML config file path", required = true)
    public var configPath: String? = null

    @Parameter(names = arrayOf("-b", "--broker"), description = "HTTP URL to broker", required = true)
    public var broker: String? = null

    @Parameter(names = arrayOf("-u", "--user"), description = "Username", required = true)
    public var user: String? = null

    @Parameter(names = arrayOf("-p", "--pass"), description = "Password", required = true)
    public var pass: String? = null

    @Parameter(names = arrayOf("-w", "--wait"), description = "Seconds to wait for broker to become available")
    public var wait = 0
}

private fun parseArguments(programName: String, argv: Array<String>): Arguments {
    val arguments = Arguments()
    val jc = JCommander()
    jc.addObject(arguments)
    jc.setProgramName(programName)

    try {
        jc.parse(*argv)
        if (arguments.help) {
            jc.usage()
            System.exit(1)
        }
    } catch (e: ParameterException) {
        log.error(e.message)
        jc.usage()
        System.exit(1)
    }

    return arguments
}

fun main(argv: Array<String>) {
    if (!run(argv)) {
        System.exit(1)
    }
}

public fun run(argv: Array<String>): Boolean {
    val arguments = parseArguments("rabbit-puppy", argv)
    log.info("Reading configuration from " + arguments.configPath!!)
    try {
        val rabbitConfig = rabbitConfigReader.read(File(arguments.configPath))
        val rabbitPuppy = RabbitPuppy(arguments.broker!!, arguments.user!!, arguments.pass!!)
        if (arguments.wait > 0) {
            rabbitPuppy.waitForBroker(arguments.wait)
        }
        rabbitPuppy.apply(rabbitConfig)
        return true
    } catch (e: RabbitConfigException) {
        log.error("Failed to read configuration, exiting")
        return false
    } catch (e: RabbitPuppyException) {
        log.error("Encountered ${e.errors.size} errors, exiting")
        return false
    }
}

