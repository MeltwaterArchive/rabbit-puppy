# rabbit-puppy
## is
A Java tool for configuring a RabbitMQ broker based on YAML configuration, which aims to be:
- easy to use
- safe to use
- unopinionated

## does
rabbit-puppy connects to a RabbitMQ broker via the HTTP API, creates resources on the broker based on a YAML configuration, and ensures that the resources specified in the configuration exist on the broker.

## does not
No destructive commands are issued.

If a _vhost_, _user_, _permission_, _queue_ or _exchange_ already exists, but with properties that differ from the configuration, it will not be deleted. Instead, an error will be logged, the tool will ignore the resource and continue, then finally indicate an error by terminating with exit code 1.

# usage
To apply `config.yaml` to a broker at `localhost` with the admin user `guest`:

```
java -jar rabbit-puppy-<version>.jar apply --broker http://localhost:15672/ --user guest --pass guest --config config.yaml
```

To check that the broker is correctly configured:

```
java -jar rabbit-puppy-<version>.jar verify --broker http://localhost:15672/ --user guest --pass guest --config config.yaml
```

# configuration
A minimal configuration to create an exchange with a binding to a queue, with default values, on the default vhost `/`:

```
exchanges:
    exchange.demo@/:
        type: topic

queues:
    queue-demo@/:

bindings:
    exchange.demo@/:
      - destination: queue-demo
        destination_type: queue
        routing_key: "#"
```

A more detailed configuration, with all values specified, that also creates new vhosts, users and permissions:

```
users:
    demo_user:
        admin: true
        password: foo

vhosts:
    demo:
        tracing: true

permissions:
    demo_user@demo:
        configure: .*
        write: .*
        read: .*

exchanges:
    exchange.A@demo:
        type: topic
        durable: true
        auto_delete: true
        internal: false
        arguments:
            alternate-exchange: exchange.B
    exchange.B@demo:
        type: direct

queues:
    queue-demo@test:
        durable: true
        auto_delete: true
        arguments:
            x-message-ttl: 10000

bindings:
    exchange.A@demo:
      - destination: queue-demo
        destination_type: queue
        routing_key: "#"
        arguments:
            foo: bar
      - destination: exchange.B
        destination_type: exchange
        routing_key: ""
```

# test
Start a local `rabbitmq-server` or do:

```
cd rabbit-puppy
docker-compose up
```

Then run `mvn test`

# build
Run `mvn package`. A jar file will be built at `target/rabbit-puppy-<version>.jar`

# java api
A Java API is available for use in e.g. integration tests:

```
// Define configuration to apply
RabbitConfig rabbitConfig = new RabbitConfig()
        .addVhost("vhost", new VHostData())
        .addUser("dan", new UserData("torrance", false))
        .addPermissions("dan", "vhost", new PermissionsData())
        .addExchange("exchange.in", "vhost", new ExchangeData())
        .addExchange("exchange.out", "vhost", new ExchangeData())
        .addQueue("queue-in", "vhost", new QueueData())
        .addBinding("exchange.in", "vhost", new BindingData("queue-in", "queue", "in", new HashMap<>()))
        .addBinding("exchange.in", "vhost", new BindingData("exchange.out", "exchange", "#", new HashMap<>()));

// Connect to broker
RabbitPuppy rabbitPuppy = new RabbitPuppy(arguments.broker, arguments.user, arguments.pass);
rabbitPuppy.waitForBroker(60);

// Apply configuration
rabbitPuppy.apply(rabbitConfig);
```

You can also read RabbitConfig from a YAML file:

```
RabbitConfig rabbitConfig = new RabbitConfigReader()
        .read(new File("config.yaml"));
```

# docker
Build a docker image containing the rabbit-puppy jar by running `mvn package docker:build`

# docker-compose
You can use rabbit-puppy with docker-compose to spin up RabbitMQ brokers with applied configuration. Create a `docker-compose.yml` file like e.g.:

```
rabbitmq:
    image: rabbitmq:3.5.0-management
    ports:
        - "15672:15672"
        - "5672:5672"

rabbitpuppy:
    image: rabbit-puppy:latest
    links:
        - rabbitmq:rabbit
    volumes:
        - ./config:/config
    command: apply -b http://rabbit:15672/ -u guest -p guest -c /config/conf.yaml -w 60
```

In the folder containing `docker-compose.yml`, create the `config` directory and copy a configuration file `conf.yaml` to it. See the **configuration** section above for some example configuration files.

Then run the setup with `docker-compose up -d`. The `-d` flag ensures that the RabbitMQ container will not terminate when the rabbit-puppy container terminates after finishing its run.

To see that it worked, inspect the admin UI at [http://localhost:15672/](http://localhost:15672/) (or your docker host IP on OS X). Note that you may need to grant your rabbit user permissions to the vhost to see its exchanges and queues.

When you are done you can kill and remove the containers:

```
docker-compose kill
docker-compose rm
```

# faq
## I see Authentication errors when running rabbit-puppy
To create a queue, exchange or binding on a vhost, one of the following must be true:
- The user given as input argument to rabbit-puppy has rights to configure the resource on the vhost.
- A user with permissions to configure the resource on the vhost, and the user's password, is specified in the  configuration file.

If neither of these are true, then you will get an authentication error when attempting to create the resource.
