package de.evoila.cf.cpi.bosh;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.model.DashboardClient;
import de.evoila.cf.broker.model.Plan;
import de.evoila.cf.broker.model.ServerAddress;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.repository.PlatformRepository;
import de.evoila.cf.broker.service.CatalogService;
import de.evoila.cf.broker.service.availability.ServicePortAvailabilityVerifier;
import de.evoila.cf.cpi.bosh.deployment.manifest.InstanceGroup;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import io.bosh.client.deployments.Deployment;
import io.bosh.client.errands.ErrandSummary;
import io.bosh.client.tasks.Task;
import io.bosh.client.vms.Vm;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;
import rx.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Service
@ConditionalOnBean(BoshProperties.class)
public class KafkaBoshPlatformService extends BoshPlatformService {

    public static final int KAFKA_PORT = 9092;
    public static final int KAFKA_PORT_SSL = 9093;
    public static final int ZOOKEEPER_PORT = 2181;
    private static final String KAFKA_JOB_NAME = "kafka";
    private static final String KAFKA_INSTANCE_GROUP_NAME = "kafka";


    KafkaBoshPlatformService(PlatformRepository repository, CatalogService catalogService, ServicePortAvailabilityVerifier availabilityVerifier, BoshProperties boshProperties, Optional<DashboardClient> dashboardClient) {
        super(repository, catalogService, availabilityVerifier, boshProperties, dashboardClient, new KafkaDeploymentManager(boshProperties));
    }

    public void runCreateErrands(ServiceInstance instance, Plan plan, Deployment deployment, Observable<List<ErrandSummary>> errands) throws PlatformException {
        Task task = super.connection.connection().errands().runErrand(deployment.getName(), "kafka-smoke-tests").toBlocking().first();
        super.waitForTaskCompletion(task);

    }

    @Override
    protected void updateHosts(ServiceInstance in, Plan plan, Deployment deployment) {

        List<Vm> vms = connection.connection().vms().listDetails(BoshPlatformService.DEPLOYMENT_NAME_PREFIX + in.getId()).toBlocking().first();
        if (in.getHosts() == null)
            in.setHosts(new ArrayList<>());

        in.getHosts().clear();

        vms.forEach(vm -> {
            ServerAddress serverAddress;

            if (vm.getJobName().equals(KAFKA_JOB_NAME)) {
                serverAddress = new ServerAddress("Kafka-" + vm.getIndex(), vm.getIps().get(0), KAFKA_PORT);
            } else {
                serverAddress = new ServerAddress("Zookeeper-" + vm.getIndex(), vm.getIps().get(0), ZOOKEEPER_PORT);
            }

            in.getHosts().add(serverAddress);
        });
    }

    @Override
    public void postDeleteInstance(ServiceInstance serviceInstance) throws PlatformException {
    }

    public void createKafkaUser(ServiceInstance instance, Plan plan, String username, String password) throws IOException, InstanceGroupNotFoundException, JSchException {
        Deployment deployment = super.getDeployment(instance);
        Manifest manifest = super.getDeployedManifest(deployment.getName());

        Optional<InstanceGroup> group = manifest.getInstanceGroups()
                .stream()
                .filter(i -> i.getName().equals(KAFKA_INSTANCE_GROUP_NAME))
                .findAny();

        System.out.println("SSH logs after this ####################################################################################");
        if (group.isPresent()) {
            int instance_cnt = group.get().getInstances();
            for (int i = 0; i < instance_cnt; i++) {
                System.out.println(group.get().getName());
                if (group.get().getName().equals(KAFKA_INSTANCE_GROUP_NAME)) {
                    createKafkaUser(instance, group.get(), i, username, password);
                }
            }
        } else {
            throw new InstanceGroupNotFoundException(instance, manifest, KAFKA_INSTANCE_GROUP_NAME);
        }
    }

    public void createKafkaUser(ServiceInstance instance, InstanceGroup instanceGroup, int i, String username, String password) throws JSchException {
        Session session = getSshSession(instance, instanceGroup, i)
                .toBlocking()
                .first();

        session.connect();
        Channel channel = session.openChannel("shell");
        channel.connect();

        List<String> commands = Arrays.asList(
//                String.format("sudo /var/vcap/packages/pgpool2/bin/pg_md5 --md5auth " +
//                                "--config-file /var/vcap/jobs/pgpool/config/pgpool.conf --username=%s %s",
//                        username, password)
                String.format("touch createUserTest")
        );

        executeCommands(channel, commands);

        close(channel, session);
    }

    private void executeCommands(Channel channel, List<String> commands){
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

    private void readChannelOutput(Channel channel){
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

    public void close(Channel channel, Session session){
        channel.disconnect();
        session.disconnect();
        log.info("Disconnected channel and session");
    }
}
