package com.meltwater.puppy

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.ParameterException
import com.beust.jcommander.Parameters
import com.meltwater.puppy.config.reader.RabbitConfigException
import com.meltwater.puppy.config.reader.RabbitConfigReader
import org.slf4j.LoggerFactory
import java.io.File

class RabbitPuppyException(s: String, val errors: List<Throwable>) : Exception(s)

private val log = LoggerFactory.getLogger("Main")

private val rabbitConfigReader = RabbitConfigReader()


class CommandMain {
    @Parameter(names = arrayOf("-h", "--help"), description = "Print help and exit", help = true)
    var help: Boolean = false
}

@Parameters(commandDescription = "Apply configuration to broker")
class CommandApply {
    @Parameter(names = arrayOf("-c", "--config"), description = "YAML config file path", required = true)
    var configPath: String? = null

    @Parameter(names = arrayOf("-b", "--broker"), description = "HTTP URL to broker", required = true)
    var broker: String? = null

    @Parameter(names = arrayOf("-u", "--user"), description = "Username", required = true)
    var user: String? = null

    @Parameter(names = arrayOf("-p", "--pass"), description = "Password", required = true)
    var pass: String? = null

    @Parameter(names = arrayOf("-w", "--wait"), description = "Seconds to wait for broker to become available")
    var wait = 0
}

@Parameters(commandDescription = "Verify broker configuration")
class CommandVerify {
    @Parameter(names = arrayOf("-c", "--config"), description = "YAML config file path", required = true)
    var configPath: String? = null

    @Parameter(names = arrayOf("-b", "--broker"), description = "HTTP URL to broker", required = true)
    var broker: String? = null

    @Parameter(names = arrayOf("-u", "--user"), description = "Username", required = true)
    var user: String? = null

    @Parameter(names = arrayOf("-p", "--pass"), description = "Password", required = true)
    var pass: String? = null

    @Parameter(names = arrayOf("-w", "--wait"), description = "Seconds to wait for broker to become available")
    var wait = 0
}

fun main(argv: Array<String>) {
    if (!Run().run("rabbit-puppy", argv)) {
        System.exit(1)
    }
}

class Run {
    fun run(programName: String, argv: Array<String>): Boolean {
        var commandMain = CommandMain()
        val commandApply = CommandApply()
        val commandVerify = CommandVerify()
        val jc = JCommander(commandMain)
        jc.addCommand("apply", commandApply)
        jc.addCommand("verify", commandVerify)
        jc.setProgramName(programName)

        try {
            jc.parse(*argv)
            if (commandMain.help || jc.parsedCommand == null) {
                jc.usage()
                System.exit(1)
            }
        } catch (e: ParameterException) {
            log.error(e.message)
            jc.usage()
            System.exit(1)
        }

        if (jc.parsedCommand.equals("apply")) {
            return apply(commandApply);
        } else if (jc.parsedCommand.equals("verify")) {
            return verify(commandVerify);
        } else {
            return false;
        }
    }
}

private fun apply(command: CommandApply): Boolean {
    log.info("Reading configuration from " + command.configPath!!)
    try {
        val rabbitConfig = rabbitConfigReader.read(File(command.configPath))
        val rabbitPuppy = RabbitPuppy(command.broker!!, command.user!!, command.pass!!)
        if (command.wait > 0) {
            rabbitPuppy.waitForBroker(command.wait)
        }
        rabbitPuppy.apply(rabbitConfig)
        return true
    } catch (e: RabbitConfigException) {
        log.error("Failed to read configuration, exiting")
        return false
    } catch (e: RabbitPuppyException) {
        val errs = StringBuilder()
        e.errors.forEach { ex -> errs.append("\n - ${ex.message}") }
        log.error("Encountered ${e.errors.size} errors, exiting: ${errs.toString()}")
        return false
    }
}

private fun verify(command: CommandVerify): Boolean {
    log.info("Reading configuration from " + command.configPath!!)
    try {
        val rabbitConfig = rabbitConfigReader.read(File(command.configPath))
        val rabbitPuppy = RabbitPuppy(command.broker!!, command.user!!, command.pass!!)
        if (command.wait > 0) {
            rabbitPuppy.waitForBroker(command.wait)
        }
        log.info("Verifying")
        rabbitPuppy.verify(rabbitConfig)
        return true
    } catch (e: RabbitConfigException) {
        log.error("Failed to read configuration, exiting")
        return false
    } catch (e: RabbitPuppyException) {
        val errs = StringBuilder()
        e.errors.forEach { ex -> errs.append("\n - ${ex.message}") }
        log.error("Encountered ${e.errors.size} errors, exiting: ${errs.toString()}")
        return false
    }
}


