package de.evoila.cf.cpi.bosh;

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
import de.evoila.cf.cpi.bosh.deployment.manifest.InstanceGroup;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import de.evoila.cf.security.credentials.credhub.CredhubClient;
import io.bosh.client.deployments.Deployment;
import io.bosh.client.errands.ErrandSummary;
import io.bosh.client.tasks.Task;
import io.bosh.client.vms.Vm;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import rx.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Service
@ConditionalOnBean(BoshProperties.class)
public class KafkaBoshPlatformService extends BoshPlatformService {

    public static final int KAFKA_PORT_SSL = 9093;
    public static final int ZOOKEEPER_PORT = 2181;
    private static final String KAFKA_JOB_NAME = "kafka";

    private CredhubClient credhubClient;


    KafkaBoshPlatformService(PlatformRepository repository, CatalogService catalogService, ServicePortAvailabilityVerifier availabilityVerifier,
                             BoshProperties boshProperties, Optional<DashboardClient> dashboardClient, Environment environment, CredhubClient credhubClient) {
        super(repository, catalogService, availabilityVerifier, boshProperties, dashboardClient, new KafkaDeploymentManager(boshProperties, environment, credhubClient));
        this.credhubClient = credhubClient;
    }

    public void runCreateErrands(ServiceInstance instance, Plan plan, Deployment deployment, Observable<List<ErrandSummary>> errands) throws PlatformException {
        Task task = boshClient.client().errands().runErrand(deployment.getName(), "kafka-smoke-test").toBlocking().first();
        waitForTaskCompletion(task);
    }

    @Override
    protected void updateHosts(ServiceInstance serviceInstance, Plan plan, Deployment deployment) {
        List<Vm> vms = super.getVms(serviceInstance);

        serviceInstance.getHosts().clear();

        vms.forEach(vm -> {
            ServerAddress serverAddress;

            if (vm.getJobName().equals(KAFKA_JOB_NAME)) {
                serverAddress = new ServerAddress("Kafka-" + vm.getIndex(), vm.getIps().get(0), KAFKA_PORT_SSL);
            } else {
                serverAddress = new ServerAddress("Zookeeper-" + vm.getIndex(), vm.getIps().get(0), ZOOKEEPER_PORT);
            }

            serviceInstance.getHosts().add(serverAddress);
        });
    }

    @Override
    public void postDeleteInstance(ServiceInstance serviceInstance) throws PlatformException {
        credhubClient.deleteCredentials(serviceInstance.getId(), "admin_password");
        credhubClient.deleteCertificate(serviceInstance.getId(), "transport_ssl");
    }

    public void createKafkaUser(ServiceInstance serviceInstance, Plan plan, String username, String password)
            throws IOException, InstanceGroupNotFoundException, JSchException {
        Manifest manifest = super.getDeployedManifest(serviceInstance);

        Optional<InstanceGroup> group = manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals("kafka"))
                .findAny();

        if(group.isPresent()) {
            createKafkaUser(serviceInstance, group.get(), username, password);
        } else {
            throw new InstanceGroupNotFoundException(serviceInstance, manifest, group.get().getName());
        }
    }

    private void createKafkaUser(ServiceInstance instance, InstanceGroup instanceGroup, String username,
                                 String password) throws JSchException {

        Session sshSession = getSshSession(instance, instanceGroup, 0)
                .toBlocking()
                .first();

        sshSession.connect();
        Channel channel = sshSession.openChannel("shell");
        channel.connect();

        List<String> commands = Arrays.asList(
                String.format("sudo /var/vcap/jobs/kafka/bin/add_user.sh %s %s", username, password)
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

        if(group.isPresent()) {
            deleteKafkaUser(serviceInstance, group.get(), username);
        } else {
            throw new InstanceGroupNotFoundException(serviceInstance, manifest, group.get().getName());
        }
    }

    private void deleteKafkaUser(ServiceInstance instance, InstanceGroup instanceGroup, String username) throws JSchException {
        Session sshSession = getSshSession(instance, instanceGroup, 0)
                .toBlocking()
                .first();

        sshSession.connect();
        Channel channel = sshSession.openChannel("shell");
        channel.connect();

        List<String> commands = Arrays.asList(
                String.format("sudo /var/vcap/jobs/kafka/bin/remove_user.sh %s", username)
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

        } catch(Exception e) {
            log.info("An error ocurred during executeCommands: "+e);
        }
    }

    private void sendCommands(Channel channel, List<String> commands){
        try {
            PrintStream out = new PrintStream(channel.getOutputStream());

            out.println("#!/bin/bash");
            for(String command : commands) {
                out.println(command);
            }
            out.println("exit");

            out.flush();
        } catch(Exception e) {
            log.info("Error while sending commands: "+ e);
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

                if(line.contains("logout")) {
                    break;
                }

                if (channel.isClosed()) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee){}
            }
        } catch(Exception e) {
            log.info("Error while reading channel output: "+ e);
        }
    }

    public void close(Channel channel, Session session) {
        channel.disconnect();
        session.disconnect();
        log.info("Disconnected channel and session");
    }
}
