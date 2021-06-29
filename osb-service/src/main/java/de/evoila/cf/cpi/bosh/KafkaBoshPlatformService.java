package de.evoila.cf.cpi.bosh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.model.DashboardClient;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.repository.PlatformRepository;
import de.evoila.cf.broker.service.CatalogService;
import de.evoila.cf.broker.service.availability.ServicePortAvailabilityVerifier;
import de.evoila.cf.cpi.CredentialConstants;
import de.evoila.cf.cpi.bosh.deployment.manifest.InstanceGroup;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import de.evoila.cf.security.credentials.CredentialStore;
import io.bosh.client.deployments.Deployment;
import io.bosh.client.errands.ErrandSummary;
import io.bosh.client.tasks.Task;
import io.bosh.client.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import rx.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@ConditionalOnBean(BoshProperties.class)
public class KafkaBoshPlatformService extends BoshPlatformService {

    public static final int KAFKA_PORT = 9092;
    public static final int KAFKA_PORT_SSL = 9093;
    public static final int ZOOKEEPER_PORT = 2181;
    private static final String KAFKA_JOB_NAME = "kafka";
    private static final String SECURE_CLIENT = "setup_secure_client_connection";

    private CredentialStore credentialStore;

    private ObjectMapper objectMapper;

    private Logger log = LoggerFactory.getLogger(KafkaBoshPlatformService.class);

    KafkaBoshPlatformService(PlatformRepository repository, CatalogService catalogService, ServicePortAvailabilityVerifier availabilityVerifier,
                             BoshProperties boshProperties, Optional<DashboardClient> dashboardClient, Environment environment, CredentialStore credentialStore) {
        super(repository, catalogService, availabilityVerifier,
                boshProperties, dashboardClient,
                new KafkaDeploymentManager(boshProperties, environment));
        this.credentialStore = credentialStore;
        this.objectMapper = new ObjectMapper(new YAMLFactory());
    }


    public void runUpdateErrands(ServiceInstance instance, Plan plan, Deployment deployment, Observable<List<ErrandSummary>> oErrands) throws PlatformException {
        runCreateErrands(instance,plan, deployment, oErrands);
    }
    
    public void runCreateErrands(ServiceInstance instance, Plan plan, Deployment deployment, Observable<List<ErrandSummary>> oErrands) throws PlatformException {
        Map<String, Object> kafka = (Map<String, Object>) instance.getParameters().get("kafka");

        if (kafka != null) {
            ArrayList<String> runErrands = (ArrayList<String>) kafka.get("errands");
            if (runErrands != null) {
                List<String> errands = oErrands.toBlocking().first().stream()
                        .map(errand -> {
                            return errand.getName();
                        })
                        .filter(errand -> {
                            return runErrands.contains(errand);
                        }).collect(Collectors.toList());

                if (errands.size() != runErrands.size()) {
                    throw new PlatformException("Errand should run not exist");
                }

                for (String errand : errands) {
                    Task task = boshClient.client().errands().runErrand(deployment.getName(), errand).toBlocking().first();
                    waitForTaskCompletion(task, Instant.now().plusSeconds(1800));
                    if ( !task.getState().equals("done") || task.getResult() == null || task.equals("done")) {
                        throw new PlatformException("Errand " + errand + " failed");
                    }
                }
            }
        }
    }

    @Override
    protected void updateHosts(ServiceInstance serviceInstance, Plan plan, Deployment deployment) {
        List<Vm> vms = super.getVms(serviceInstance);

        serviceInstance.getHosts().clear();

        vms.forEach(vm -> {
            ServerAddress serverAddress;

            if (vm.getJobName().equals(KAFKA_JOB_NAME)) {

                Manifest manifest = null;
                try {
                    manifest = objectMapper.readValue(deployment.getRawManifest(), Manifest.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (isKafkaSecure(manifest)) {
                    serverAddress = new ServerAddress("Kafka-" + vm.getIndex(), vm.getIps().get(0), KAFKA_PORT_SSL);
                } else {
                    serverAddress = new ServerAddress("Kafka-" + vm.getIndex(), vm.getIps().get(0), KAFKA_PORT);
                }
            } else {
                serverAddress = new ServerAddress("Zookeeper-" + vm.getIndex(), vm.getIps().get(0), ZOOKEEPER_PORT);
            }

            serviceInstance.getHosts().add(serverAddress);
        });
    }

    @Override
    public void postDeleteInstance(ServiceInstance serviceInstance) {
        try {
            credentialStore.deleteCredentials(serviceInstance, CredentialConstants.ADMIN_PASSWORD);
            credentialStore.deleteCertificate(serviceInstance, CredentialConstants.TRANSPORT_SSL);
        } catch (Exception ex) {
            log.info("Could not delete Credentials: this might happen, when Kafka Security was not configured");
        }
    }

    public boolean isKafkaSecure(Manifest manifest) {
        Map<String, Object> kafkaProperties = (Map<String, Object>) manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals(KAFKA_JOB_NAME))
                .findAny()
                .get()
                .getProperties()
                .get("kafka");

        Map<String, Object> kafkaSecurityProperties = (Map<String, Object>) kafkaProperties.get("security");

        return (Boolean) kafkaSecurityProperties.get(SECURE_CLIENT);
    }

    public void createKafkaUser(ServiceInstance serviceInstance, Plan plan, String username, String password)
            throws IOException, InstanceGroupNotFoundException, JSchException {
        Manifest manifest = super.getDeployedManifest(serviceInstance);

        Optional<InstanceGroup> group = manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals("kafka"))
                .findAny();

        if (group.isPresent()) {
            //createKafkaUser(serviceInstance, group.get(), username, password, topic, groups, cluster);
            createKafkaUser(serviceInstance, group.get(), username, password);
        } else {
            throw new InstanceGroupNotFoundException(serviceInstance, manifest, group.get().getName());
        }
    }

    private void createKafkaUser(ServiceInstance instance, InstanceGroup instanceGroup, String username,
                                 String password) throws JSchException {

        Session sshSession = getSshSession(instance, instanceGroup.getName(), 0)
                .toBlocking()
                .first();

        sshSession.connect();
        Channel channel = sshSession.openChannel("shell");
        channel.connect();

        List<String> commands = Arrays.asList(
                //String.format("sudo /var/vcap/jobs/kafka/bin/add_user.sh '%s' '%s' '%s' '%s' '%s'", username, password, topics, groups, cluster)
                String.format("sudo /var/vcap/jobs/kafka/bin/add_user.sh '%s' '%s' '%s' '%s' '%s'", username, password)
        );

        executeCommands(channel, commands);

        close(channel, sshSession);
    }

    public void deleteKafkaUser(ServiceInstance serviceInstance, Plan plan, String username) throws IOException, InstanceGroupNotFoundException, JSchException {
        Manifest manifest = super.getDeployedManifest(serviceInstance);

        Optional<InstanceGroup> group = manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals("kafka"))
                .findAny();

        if (group.isPresent()) {
            //deleteKafkaUser(serviceInstance, group.get(), username, topics, groups, cluster);
            deleteKafkaUser(serviceInstance, group.get(), username);
        } else {
            throw new InstanceGroupNotFoundException(serviceInstance, manifest, group.get().getName());
        }
    }

    private void deleteKafkaUser(ServiceInstance instance, InstanceGroup instanceGroup, String username) throws JSchException {
        Session sshSession = getSshSession(instance, instanceGroup.getName(), 0)
                .toBlocking()
                .first();

        sshSession.connect();
        Channel channel = sshSession.openChannel("shell");
        channel.connect();

        List<String> commands = Arrays.asList(
                String.format("sudo /var/vcap/jobs/kafka/bin/remove_user.sh '%s' '%s' '%s' '%s'", username)
        );

        executeCommands(channel, commands);

        close(channel, sshSession);

    }

    private void executeCommands(Channel channel, List<String> commands) {
        try {

            log.info("Sending commands...");
            sendCommands(channel, commands);

            readChannelOutput(channel);
            log.info("Finished sending commands!");

        } catch (Exception e) {
            log.info("An error ocurred during executeCommands: " + e);
        }
    }

    private void sendCommands(Channel channel, List<String> commands) {
        try {
            PrintStream out = new PrintStream(channel.getOutputStream());

            out.println("#!/bin/bash");
            for (String command : commands) {
                out.println(command);
            }
            out.println("exit");

            out.flush();
        } catch (Exception e) {
            log.info("Error while sending commands: " + e);
        }

    }

    private void readChannelOutput(Channel channel) {
        byte[] buffer = new byte[1024];

        try {
            InputStream in = channel.getInputStream();
            String line = "";
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(buffer, 0, 1024);
                    if (i < 0) {
                        break;
                    }
                    line = new String(buffer, 0, i);
                    log.info(line);
                }

                if (line.contains("logout")) {
                    break;
                }

                if (channel.isClosed()) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
        } catch (Exception e) {
            log.info("Error while reading channel output: " + e);
        }
    }

    public void close(Channel channel, Session session) {
        channel.disconnect();
        session.disconnect();
        log.info("Disconnected channel and session");
    }
}
