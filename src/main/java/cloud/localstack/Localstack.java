package cloud.localstack;

import cloud.localstack.docker.*;
import cloud.localstack.docker.command.*;
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration;
import cloud.localstack.docker.exception.LocalstackDockerException;

import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Localstack Docker instance
 *
 * @author Alan Bevier
 * @author fabianoo
 */
public class Localstack {

    public static final String ENV_CONFIG_USE_SSL = "USE_SSL";
    public static final String ENV_CONFIG_EDGE_PORT = "EDGE_PORT";

    private static final Logger LOG = Logger.getLogger(Localstack.class.getName());

    private static final Pattern READY_TOKEN = Pattern.compile("Ready\\.");

    private static final int DEFAULT_EDGE_PORT = 4566;

    private static final String PORT_CONFIG_FILENAME = "/opt/code/localstack/" +
            ".venv/lib/python3.8/site-packages/localstack_client/config.py";

    //Regular expression used to parse localstack config to determine default ports for services
    private static final Pattern DEFAULT_PORT_PATTERN = Pattern.compile("'(\\w+)'\\Q: '{proto}://{host}:\\E(\\d+)'");

    private Container localStackContainer;

    /**
     * This is a mapping from service name to internal ports.  In order to use them, the
     * internal port must be resolved to an external docker port via Container.getExternalPortFor()
     */
    private static Map<String, Integer> serviceToPortMap;

    private static boolean locked = false;

    public static final Localstack INSTANCE = new Localstack();

    private String externalHostName;

    static {
        // make sure we avoid any errors related to locally generated SSL certificates
        CommonUtils.disableSslCertChecking();
    }

    private Localstack() { }

    public void startup(LocalstackDockerConfiguration dockerConfiguration) {
        if (locked) {
            throw new IllegalStateException("A docker instance is starting or already started.");
        }
        locked = true;
        this.externalHostName = dockerConfiguration.getExternalHostName();

        try {
            localStackContainer = Container.createLocalstackContainer(
                dockerConfiguration.getExternalHostName(),
                dockerConfiguration.isPullNewImage(),
                dockerConfiguration.isRandomizePorts(),
                dockerConfiguration.getImageTag(),
                dockerConfiguration.getEnvironmentVariables(),
                dockerConfiguration.getPortMappings()
            );
            loadServiceToPortMap();

            LOG.info("Waiting for LocalStack container to be ready...");
            localStackContainer.waitForLogToken(READY_TOKEN);
        } catch (Exception t) {
            if (true) {
                LOG.info("Ignoring port conflict when starting Docker container, due to ignoreDockerRunErrors=true");
                localStackContainer = Container.getRunningLocalstackContainer();
                loadServiceToPortMap();
                return;
            }
            this.stop();
            throw new LocalstackDockerException("Could not start the localstack docker container.", t);
        }
    }

    public void stop() {
        if (localStackContainer != null) {
            localStackContainer.stop();
        }
        locked = false;
    }

    public boolean isRunning() {
        return localStackContainer == null ? false : localStackContainer.isRunning();
    }

    private void loadServiceToPortMap() {
        String localStackPortConfig = localStackContainer.executeCommand(Arrays.asList("cat", PORT_CONFIG_FILENAME));

        int edgePort = getEdgePort();
        Map<String, Integer> ports = new RegexStream(DEFAULT_PORT_PATTERN.matcher(localStackPortConfig)).stream()
                .collect(Collectors.toMap(match -> match.group(1), match -> edgePort));

        serviceToPortMap = Collections.unmodifiableMap(ports);
    }

    public String getEndpointS3() {
        String s3Endpoint = endpointForService(ServiceName.S3);
        /*
         * Use the domain name wildcard *.localhost.localstack.cloud which maps to 127.0.0.1
         * We need to do this because S3 SDKs attempt to access a domain <bucket-name>.<service-host-name>
         * which by default would result in <bucket-name>.localhost, but that name cannot be resolved
         * (unless hardcoded in /etc/hosts)
         */
        s3Endpoint = s3Endpoint.replace("localhost", Constants.LOCALHOST_DOMAIN_NAME);
        return s3Endpoint;
    }

    public int getEdgePort() {
        String envEdgePort = System.getenv(ENV_CONFIG_EDGE_PORT);
        return envEdgePort == null ? DEFAULT_EDGE_PORT : Integer.parseInt(envEdgePort);
    }

    public String getEndpointKinesis() {
        return endpointForService(ServiceName.KINESIS);
    }

    public String getEndpointLambda() {
        return endpointForService(ServiceName.LAMBDA);
    }

    public String getEndpointDynamoDB() {
        return endpointForService(ServiceName.DYNAMO);
    }

    public String getEndpointDynamoDBStreams() {
        return endpointForService(ServiceName.DYNAMO_STREAMS);
    }

    public String getEndpointAPIGateway() {
        return endpointForService(ServiceName.API_GATEWAY);
    }

    public String getEndpointElasticsearch() {
        return endpointForService(ServiceName.ELASTICSEARCH);
    }

    public String getEndpointElasticsearchService() {
        return endpointForService(ServiceName.ELASTICSEARCH_SERVICE);
    }

    public String getEndpointFirehose() {
        return endpointForService(ServiceName.FIREHOSE);
    }

    public String getEndpointSNS() {
        return endpointForService(ServiceName.SNS);
    }

    public String getEndpointSQS() {
        return endpointForService(ServiceName.SQS);
    }

    public String getEndpointRedshift() {
        return endpointForService(ServiceName.REDSHIFT);
    }

    public String getEndpointCloudWatch() {
        return endpointForService(ServiceName.CLOUDWATCH);
    }

    public String getEndpointCloudWatchLogs() {
        return endpointForService(ServiceName.CLOUDWATCH_LOGS);
    }

    public String getEndpointSES() {
        return endpointForService(ServiceName.SES);
    }

    public String getEndpointRoute53() {
        return endpointForService(ServiceName.ROUTE53);
    }

    public String getEndpointCloudFormation() {
        return endpointForService(ServiceName.CLOUDFORMATION);
    }

    public String getEndpointSSM() {
        return endpointForService(ServiceName.SSM);
    }

    public String getEndpointSecretsmanager() {
        return endpointForService(ServiceName.SECRETSMANAGER);
    }

    public String getEndpointEC2() {
        return endpointForService(ServiceName.EC2);
    }

    public String getEndpointStepFunctions() {
        return endpointForService(ServiceName.STEPFUNCTIONS);
    }

    public String getEndpointIAM() {
        return endpointForService(ServiceName.IAM);
    }

    public String endpointForService(String serviceName) {
        return endpointForPort(getServicePort(serviceName));
    }

    public int getServicePort(String serviceName) {
        if (serviceToPortMap == null) {
            throw new IllegalStateException("Service to port mapping has not been determined yet.");
        }

        if (!serviceToPortMap.containsKey(serviceName)) {
            throw new IllegalArgumentException("Unknown port mapping for service: " + serviceName);
        }

        return serviceToPortMap.get(serviceName);
    }

    public String endpointForPort(int port) {
        if (localStackContainer != null) {
            int externalPort = localStackContainer.getExternalPortFor(port);
            String protocol = useSSL() ? "https" : "http";
            return String.format("%s://%s:%s", protocol, externalHostName, externalPort);
        }

        throw new RuntimeException("Container not started");
    }

    public Container getLocalStackContainer() {
        return localStackContainer;
    }

    public static boolean useSSL() {
        return isEnvConfigSet(ENV_CONFIG_USE_SSL);
    }

    public static boolean isEnvConfigSet(String configName) {
        String value = System.getenv(configName);
        return value != null && !Arrays.asList("false", "0", "").contains(value.trim());
    }

    public static String getDefaultRegion() {
        return Constants.DEFAULT_REGION;
    }
}
