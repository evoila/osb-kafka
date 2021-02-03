package de.evoila.cf.broker.custom.kafka;

import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.exception.ServiceBrokerException;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import de.evoila.cf.cpi.bosh.KafkaBoshPlatformService;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author Patrick Weber.
 *
 * Importent !!!!!
 * Not Working because Sasl Jaas conifg not accepted by Zookeper and Kafka Client
 */


public class KafkaAdminTools {

    ArrayList<ServerAddress> zookeeper=new ArrayList<ServerAddress>();
    ArrayList<ServerAddress> kafka=new ArrayList<ServerAddress>();
    boolean secure;
    String username;
    String password;
    String fileName;
    BufferedWriter jaas;


    private Logger log = LoggerFactory.getLogger(KafkaAdminTools.class);


    KafkaAdminTools(List<ServerAddress> serverAddresses, boolean secure, String username, String password) throws IOException, ServiceBrokerException {
        throw new ServiceBrokerException("Class not working beclause Sasl problems");
        this.secure = secure;
        this.username = username;
        this.password = password;
        serverAddresses.stream().forEach(server ->{
            if (server.getPort() == KafkaBoshPlatformService.KAFKA_PORT || server.getPort() == KafkaBoshPlatformService.KAFKA_PORT_SSL) {
                kafka.add(server);
            } else {
                zookeeper.add(server);
            }
        });
        this.fileName =
        System.setProperty("java.security.auth.login.config","/tmp/kafka-" + UUID.randomUUID().toString());
        this.jaas = new BufferedWriter(new FileWriter(System.getProperty("java.security.auth.login.config"), true));
        jaas.write("Client {\n" + 
                "security.protocol=SASL_SSL\n" +
                "ssl.truststore.password=kafka-sec\n" +
                "sasl.mechanism=SCRAM-SHA-256\n" +
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + this.username + "\"  password=\"" + this.password + "\" ;\n" +
                "}\n");
        jaas.flush();

    }

    void createUser(String username, String password) throws PlatformException, NoSuchAlgorithmException, ServiceBrokerException {
        boolean success = false;
        ZKClientConfig zkClientConfig = new ZKClientConfig();
        if (secure) {
            zkClientConfig.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY,"true");
            zkClientConfig.setProperty(ZKClientConfig.ZK_SASL_CLIENT_USERNAME,"admin");
            zkClientConfig.setProperty(ZKClientConfig.SECURE_CLIENT,"true");

        }
        for (ServerAddress server: zookeeper) {
            KafkaZkClient zkClient = KafkaZkClient.apply(server.getIp()+":"+server.getPort(), false, 10000, 5000,10, Time.SYSTEM,"admin","osb",null,Option.apply(zkClientConfig));
            if (zkClient == null){
                continue;
            }
            AdminZkClient adminZkClient = new AdminZkClient(zkClient);
            if (adminZkClient != null) {
                HashMap<String, String> userProp = new HashMap<>();
                ScramCredential scramCredential = new ScramFormatter(ScramMechanism.SCRAM_SHA_256).generateCredential(password, 4096);
                userProp.put("SCRAM-SHA-256", ScramCredentialUtils.credentialToString(scramCredential));
                Properties configs = adminZkClient.fetchEntityConfig(ConfigType.User(), username);
                configs.putAll(userProp);
                adminZkClient.changeConfigs(ConfigType.User(), username, configs);
                success = true;
            }
        }
        if (!success){
            throw new PlatformException("Can not create user");
        }
    }

    void deleteUser(String username) throws PlatformException {
        boolean success = false;
        ZKClientConfig zkClientConfig = new ZKClientConfig();
        if (secure) {
            zkClientConfig.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT,"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +this.username  + "\" password=\"" + this.password +"\"admin_user=\""+this.password+"\";");
        }
        for (ServerAddress server: zookeeper) {
            KafkaZkClient zkClient = KafkaZkClient.apply(server.getIp()+":"+server.getPort(), secure, 10000, 5000,10, Time.SYSTEM,"admin","osb",null,Option.apply(zkClientConfig));
            if (zkClient == null){
                continue;
            }
            AdminZkClient adminZkClient = new AdminZkClient(zkClient);
            if (adminZkClient != null) {
                adminZkClient.changeConfigs(ConfigType.User(), username, new Properties());
                success = true;
            }

        }
        if (!success){
            throw new PlatformException("Can not create user");
        }
    }

    void createAcl(String username, Map<String,Object> permissions) throws PlatformException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka.stream().map(server -> {
            return server.getIp() + ":" + server.getPort();
        }).collect(Collectors.joining(",")));
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        if (secure) {
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.mechanism", "SCRAM-SHA-256");
            properties.put("listener.name.sasl_ssl.scram-sha-256.sasl..jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\"  password=\"" + password + "\" ;");
        }
        AdminClient kafkaAdminClient = KafkaAdminClient.create(properties);
        if (kafkaAdminClient != null){
            throw  new PlatformException("Can not connect to Kafka");
        }
        ArrayList<AclBinding> aclBindings = new ArrayList<AclBinding>();
        log.error("permissons"+ permissions + " " + permissions.size());
        permissions.forEach((type, value) -> {
            log.error("ResourceType("+type+")");
            Map<String, Object> map = (Map<String, Object>) value;
            String name = (String) map.get("name");
            ((ArrayList<String>) map.get("rights")).forEach(right -> {
                log.error("ResourceType("+type+"): "+ ResourceType.fromString(type) + ";Resourece("+name+"):" + PatternType.fromString(name)+ ";Rights("+right+"):"+AclOperation.fromString(right));
                aclBindings.add(new AclBinding(new ResourcePattern(ResourceType.fromString(type), "*", PatternType.fromString(name)), new AccessControlEntry(username, "*", AclOperation.fromString(right), AclPermissionType.ALLOW)));
            });
        });

        if (!kafkaAdminClient.createAcls(aclBindings).all().isDone()){
            throw new PlatformException("Can not create ACL");
        }
    }

    void deleteAcl(String username) throws PlatformException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka.stream().map(server -> {
            return server.getIp() + ":" + server.getPort();
        }).collect(Collectors.joining(",")));
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        if (secure) {
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.mechanism", "SCRAM-SHA-256");
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\"  password=\"" + password + "\";");
        }
        AdminClient kafkaAdminClient = KafkaAdminClient.create(properties);
        if (kafkaAdminClient != null) {
            throw new PlatformException("Can not connect to Kafka");
        }

        ArrayList<AclBindingFilter> aclBindingFilters = new ArrayList<AclBindingFilter>(1);
        aclBindingFilters.add(new AclBindingFilter(new ResourceFilter(ResourceType.ANY, "*"), new AccessControlEntryFilter(username, "*", AclOperation.ANY, AclPermissionType.ANY)));

        if (!kafkaAdminClient.deleteAcls(aclBindingFilters).all().isDone()) {
            throw new PlatformException("Can not create ACL");
        }
    }

    void clean(){ 
        new File(System.getProperty("java.security.auth.login.config")).delete();

    }
}

