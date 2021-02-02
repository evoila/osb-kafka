package de.evoila.cf.broker.custom.kafka;

import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import kafka.Kafka;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.client.ZKClientConfig;
import kafka.Kafka.*;
import scala.Option;

import java.security.acl.Acl;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaAdminTools {

    ArrayList<ServerAddress> zookeeper=new ArrayList<ServerAddress>();
    ArrayList<ServerAddress> kafka=new ArrayList<ServerAddress>();
    boolean secure;
    String username;
    String password;

    void KafkaAdminTools(List<ServerAddress> serverAddresses, int kafkaPort, int zookeeperPort, boolean secure, String username, String password){
        this.secure = secure;
        this.username = username;
        this.password = password;
        serverAddresses.stream().forEach(server ->{
            if (server.getPort() == kafkaPort) {
                kafka.add(server);
            } else {
                zookeeper.add(server);
            }
        });
    }

    void createUser(String username, String password) throws PlatformException {
        boolean success = false;
        ZKClientConfig zkClientConfig = new ZKClientConfig();
        if (secure) {
            zkClientConfig.setProperty("org.apache.kafka.common.security.plain.PlainLoginModule","required");
            zkClientConfig.setProperty("username",this.username);
            zkClientConfig.setProperty("password",this.password);
            zkClientConfig.setProperty("admin_user",this.password);
        }
        for (ServerAddress server: zookeeper) {
            KafkaZkClient zkClient = KafkaZkClient.apply(server.getIp()+":"+server.getPort(), secure, 10000, 5000,10, Time.SYSTEM,"admin","osb",null,Option.apply());
            if (zkClient == null){
                continue;
            }
            AdminZkClient adminZkClient = new AdminZkClient(zkClient);
            if (adminZkClient != null) {
                HashMap<String, String> userProp = new HashMap<>();
                ScramCredential scramCredential = new ScramFormatter(ScramMechanism.SCRAM_SHA_256).generateCredential(password, 4096);
                userProp.put("userSecureSchema", ScramCredentialUtils.credentialToString(scramCredential));
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
            zkClientConfig.setProperty("org.apache.kafka.common.security.plain.PlainLoginModule","required");
            zkClientConfig.setProperty("username",this.username);
            zkClientConfig.setProperty("password",this.password);
            zkClientConfig.setProperty("admin_user",this.password);
        }
        for (ServerAddress server: zookeeper) {
            KafkaZkClient zkClient = KafkaZkClient.apply(server.getIp()+":"+server.getPort(), secure, 10000, 5000,10, Time.SYSTEM,"admin","osb",null,Option.apply());
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

    void createAcl(String username, Map<String,Object> premissions) throws PlatformException {
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
        if (kafkaAdminClient != null){
            throw  new PlatformException("Can not connect to Kafka");
        }
        ArrayList<AclBinding> aclBindings = new ArrayList<AclBinding>();
        premissions.forEach((type, value) -> {
            Map<String, Object> map = (Map<String, Object>) value;
            String name = (String) map.get("name");
            ((ArrayList<String>) map.get("rights")).forEach(right -> {
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
        if (kafkaAdminClient != null){
            throw  new PlatformException("Can not connect to Kafka");
        }

        ArrayList<AclBindingFilter> aclBindingFilters= new ArrayList<AclBindingFilter>(1);
        aclBindingFilters.add(new AclBindingFilter(new ResourceFilter(ResourceType.ANY,"*"),new AccessControlEntryFilter(username,"*",AclOperation.ANY,AclPermissionType.ANY)));

        if (!kafkaAdminClient.deleteAcls(aclBindingFilters).all().isDone()){
            throw new PlatformException("Can not create ACL");
        }
    }
}
