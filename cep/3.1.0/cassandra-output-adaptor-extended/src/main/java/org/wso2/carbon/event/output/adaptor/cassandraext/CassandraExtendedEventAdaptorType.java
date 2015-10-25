/*
 * Copyright 2004,2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.event.output.adaptor.cassandraext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.event.output.adaptor.cassandraext.internal.util.CassandraExtendedEventAdaptorConstants;
import org.wso2.carbon.event.output.adaptor.core.AbstractOutputEventAdaptor;
import org.wso2.carbon.event.output.adaptor.core.MessageType;
import org.wso2.carbon.event.output.adaptor.core.Property;
import org.wso2.carbon.event.output.adaptor.core.config.OutputEventAdaptorConfiguration;
import org.wso2.carbon.event.output.adaptor.core.exception.OutputEventAdaptorEventProcessingException;
import org.wso2.carbon.event.output.adaptor.core.message.config.OutputEventAdaptorMessageConfiguration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class CassandraExtendedEventAdaptorType extends AbstractOutputEventAdaptor {

    private static final Log log = LogFactory.getLog(CassandraExtendedEventAdaptorType.class);
    //    private StringSerializer sser = new StringSerializer();
    private static CassandraExtendedEventAdaptorType cassandraEventAdaptor = new CassandraExtendedEventAdaptorType();
    private ResourceBundle resourceBundle;
    private ConcurrentHashMap<Integer, ConcurrentHashMap<OutputEventAdaptorConfiguration, EventAdaptorInfo>> tenantedCassandraClusterCache = new ConcurrentHashMap<Integer, ConcurrentHashMap<OutputEventAdaptorConfiguration, EventAdaptorInfo>>();

    private CassandraExtendedEventAdaptorType() {

    }

    @Override
    protected List<String> getSupportedOutputMessageTypes() {
        List<String> supportOutputMessageTypes = new ArrayList<String>();
        supportOutputMessageTypes.add(MessageType.MAP);
        return supportOutputMessageTypes;
    }

    /**
     * @return cassandra event adaptor instance
     */
    public static CassandraExtendedEventAdaptorType getInstance() {

        return cassandraEventAdaptor;
    }

    /**
     * @return name of the cassandra event adaptor
     */
    @Override
    protected String getName() {
        return CassandraExtendedEventAdaptorConstants.ADAPTOR_TYPE_CASSANDRA;
    }

    /**
     * Initialises the resource bundle
     */
    @Override
    protected void init() {
        //To change body of implemented methods use File | Settings | File Templates.
        resourceBundle = ResourceBundle.getBundle("org.wso2.carbon.event.output.adaptor.cassandraext.i18n.Resources", Locale.getDefault());
    }


    /**
     * @return output adaptor configuration property list
     */
    @Override
    public List<Property> getOutputAdaptorProperties() {

        List<Property> propertyList = new ArrayList<Property>();

        // set cluster name
        Property clusterName = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_CLUSTER_NAME);
        clusterName.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_CLUSTER_NAME));
        clusterName.setRequired(true);
        clusterName.setHint(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_CLUSTER_NAME_HINT));
        propertyList.add(clusterName);

        // set host name
        Property hostName = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_HOSTNAME);
        hostName.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_HOSTNAME));
        hostName.setRequired(true);
        propertyList.add(hostName);


        // set port
        Property port = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PORT);
        port.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PORT));
        port.setRequired(true);
        propertyList.add(port);


        // set user name
        Property userName = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_USER_NAME);
        userName.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_USER_NAME));
        userName.setRequired(true);
        propertyList.add(userName);


        // set password
        Property password = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PASSWORD);
        password.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PASSWORD));
        password.setRequired(true);
        password.setSecured(true);
        propertyList.add(password);


        // set index all columns
        Property indexAllColumns = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_ALL_COLUMNS);
        indexAllColumns.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_ALL_COLUMNS));
        indexAllColumns.setOptions(new String[]{"true", "false"});
        indexAllColumns.setDefaultValue("false");
        indexAllColumns.setHint(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_ALL_COLUMNS_HINT));
        propertyList.add(indexAllColumns);

        return propertyList;

    }

    /**
     * @return output message configuration property list
     */
    @Override
    public List<Property> getOutputMessageProperties() {

        List<Property> propertyList = new ArrayList<Property>();

        // key space
        Property keySpace = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_KEY_SPACE_NAME);
        keySpace.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_KEY_SPACE_NAME));
        keySpace.setRequired(true);
        propertyList.add(keySpace);

        // column family
        Property columnFamily = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_COLUMN_FAMILY_NAME);
        columnFamily.setDisplayName(
                resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_COLUMN_FAMILY_NAME));
        columnFamily.setRequired(true);
        propertyList.add(columnFamily);

        Property executionMode = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_EXECUTION_MODE);
        executionMode.setDisplayName(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_EXECUTION_MODE));
        executionMode.setOptions(new String[]{resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_EXECUTION_MODE_INSERT), resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_EXECUTION_MODE_UPDATE)});
        executionMode.setHint(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_EXECUTION_MODE_HINT));
        executionMode.setRequired(true);
        propertyList.add(executionMode);

        Property indexColumns = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_KEYS);
        indexColumns.setDisplayName(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_KEYS));
        indexColumns.setHint(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_KEYS_HINT));
        propertyList.add(indexColumns);

        Property primaryKey = new Property(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PRIMARY_KEY);
        primaryKey.setDisplayName(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PRIMARY_KEY));
        primaryKey.setHint(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PRIMARY_KEY_HINT));
        propertyList.add(primaryKey);

        return propertyList;
    }

    /**
     * @param outputEventMessageConfiguration
     *                 - topic name to publish messages
     * @param message  - is and Object[]{Event, EventDefinition}
     * @param outputEventAdaptorConfiguration
     *
     * @param tenantId
     */
    public void publish(
            OutputEventAdaptorMessageConfiguration outputEventMessageConfiguration,
            Object message,
            OutputEventAdaptorConfiguration outputEventAdaptorConfiguration, int tenantId) {
        ConcurrentHashMap<OutputEventAdaptorConfiguration, EventAdaptorInfo> cassandraClusterCache = null;
        if (message instanceof Map) {
            try {

                cassandraClusterCache = tenantedCassandraClusterCache.get(tenantId);
                if (null == cassandraClusterCache) {
                    cassandraClusterCache = new ConcurrentHashMap<OutputEventAdaptorConfiguration, EventAdaptorInfo>();
                    if (null != tenantedCassandraClusterCache.putIfAbsent(tenantId, cassandraClusterCache)) {
                        cassandraClusterCache = tenantedCassandraClusterCache.get(tenantId);
                    }
                }

                String keySpaceName = outputEventMessageConfiguration.getOutputMessageProperties().get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_KEY_SPACE_NAME);
                String columnFamilyName = outputEventMessageConfiguration.getOutputMessageProperties().get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_COLUMN_FAMILY_NAME);

                EventAdaptorInfo eventAdaptorInfo = cassandraClusterCache.get(outputEventAdaptorConfiguration);
                if (null == eventAdaptorInfo) {
                    Map<String, String> properties = outputEventAdaptorConfiguration.getOutputProperties();

                    String username = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_USER_NAME);
                    String password = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PASSWORD);
                    String cassandraHost = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_HOSTNAME);
                    String cassandraPort = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PORT);
                    String clusterName = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_CLUSTER_NAME);


                    Cluster.Builder clusterBuilder = Cluster.builder()
                            .addContactPoint(cassandraHost);
                    if (cassandraPort != null && cassandraPort.length() > 0) {
                        clusterBuilder.withPort(Integer.parseInt(cassandraPort));
                    }
                    clusterBuilder.withClusterName(clusterName);
                    if (username != null && username.length() > 0) {
                        clusterBuilder.withCredentials(username, password);
                    }

                    Cluster cluster = clusterBuilder.build();

                    String indexAllColumnsString = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_ALL_COLUMNS);
                    boolean indexAllColumns = false;
                    if (indexAllColumnsString != null && indexAllColumnsString.equals("true")) {
                        indexAllColumns = true;
                    }
                    eventAdaptorInfo = new EventAdaptorInfo(cluster, indexAllColumns);
                    if (null != cassandraClusterCache.putIfAbsent(outputEventAdaptorConfiguration, eventAdaptorInfo)) {
                        eventAdaptorInfo = cassandraClusterCache.get(outputEventAdaptorConfiguration);
                    } else {
                        log.info("Initiated Cassandra Writer " + outputEventAdaptorConfiguration.getName());
                    }
                }


                MessageInfo messageInfo = eventAdaptorInfo.getMessageInfoMap().get(outputEventMessageConfiguration);

                String executionMode = outputEventMessageConfiguration.getOutputMessageProperties().get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_EXECUTION_MODE);
                String updateColKeys = outputEventMessageConfiguration.getOutputMessageProperties().get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_INDEX_KEYS);


                if (null == messageInfo) {

                    messageInfo = new MessageInfo();
                    // this is eternal and thread-safe.
                    Session session = eventAdaptorInfo.getCluster().connect(keySpaceName);

                    messageInfo.setSession(session);
                    messageInfo.setInsertOrUpdate(executionMode.equalsIgnoreCase(resourceBundle.getString(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_EXECUTION_MODE_UPDATE)));
                    if (messageInfo.isInsertOrUpdate()) {
                        ArrayList<String> keyList = new ArrayList<String>();
                        String[] keys = updateColKeys == null ? new String[0] : updateColKeys.trim().split(",");
                        for (String key : keys) {
                            keyList.add(key.trim());
                        }
                        messageInfo.setKeyColumns(keyList);
                    }
                    messageInfo.setPrimaryKey(outputEventMessageConfiguration.getOutputMessageProperties().get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PRIMARY_KEY));
                    if (null != eventAdaptorInfo.getMessageInfoMap().putIfAbsent(outputEventMessageConfiguration, messageInfo)) {
                        messageInfo = eventAdaptorInfo.getMessageInfoMap().get(outputEventMessageConfiguration);
                    }
                }


                // customized code
                Session session = messageInfo.getSession();
                Map<String, Object> attributeMap = (Map<String, Object>) message;

                // optional
                String primaryKey = messageInfo.getPrimaryKey();
                if (primaryKey != null && primaryKey.trim().length() == 0) {
                    // not configured properly.
                    primaryKey = null;
                }
                // optional
                ArrayList<String> indexCols = messageInfo.getKeyColumns();

                // create table and indexes if not exist.
                if (!messageInfo.isCfInitialized()) {
                    try {
                        session.execute("select * from " + columnFamilyName + " limit 1");
                        messageInfo.setCfInitialized(true);
                    } catch (Exception ex) {
                        // assuming table doesn't exist.

                        StringBuilder creationQuery = new StringBuilder("create table " + columnFamilyName + " (");
                        if (primaryKey == null || primaryKey.length() == 0) {
                            creationQuery.append("uuid_key text primary key, ");
                        }
                        for (String col : attributeMap.keySet()) {
                            creationQuery.append(col).append(" ").append("text");
                            if (col.equals(primaryKey)) {
                                creationQuery.append(" primary key");
                            }
                            creationQuery.append(",");
                        }
                        String query = creationQuery.substring(0, creationQuery.length() - 1) + ")";
                        session.execute(query);

                        // creating indexes
                        if (indexCols != null) {
                            for (String index : indexCols) {
                                if (!index.equals(primaryKey)) {
                                    String indexQuery = "create index ind_" + columnFamilyName + "_" + index + " on " + columnFamilyName +
                                            " (" + index + ")";
                                    session.execute(indexQuery);
                                }
                            }
                        }
                        messageInfo.setCfInitialized(true);
                    }
                }
                // end of table creation


                // inserting and updating.
                if (messageInfo.isInsertOrUpdate()) {
                    // checking whether the key cols values exist
                    StringBuilder queryBuilder = new StringBuilder("update ");
                    queryBuilder.append(columnFamilyName);
                    queryBuilder.append(" set ");

                    boolean addComma = false;
                    for (Map.Entry<String, Object> entry : attributeMap.entrySet()) {
                        if (!entry.getKey().equals(primaryKey)) {
                            if (addComma) {
                                queryBuilder.append(",");
                            }
                            queryBuilder.append(entry.getKey());
                            queryBuilder.append(" = '");
                            queryBuilder.append(entry.getValue());
                            queryBuilder.append("'");
                            addComma = true;
                        }
                    }

                    queryBuilder.append(" where ");
                    queryBuilder.append(primaryKey);
                    queryBuilder.append(" = '");
                    queryBuilder.append(attributeMap.get(primaryKey));
                    queryBuilder.append("'");

                    session.execute(queryBuilder.toString());
                } else {
                    // inserting with uuid to allow duplicates
                    // if user enters a primary key here, it will be similar to the update clause.
                    StringBuilder queryBuilder = new StringBuilder("insert into ");
                    queryBuilder.append(columnFamilyName);
                    queryBuilder.append("  (");
                    boolean addComma = false;
                    if (primaryKey == null) {
                        queryBuilder.append("uuid_key, ");
                    }
                    for (Map.Entry<String, Object> entry : attributeMap.entrySet()) {
                        if (addComma) {
                            queryBuilder.append(", ");
                        }
                        queryBuilder.append(entry.getKey());
                        addComma = true;
                    }

                    queryBuilder.append(") values (");
                    if (primaryKey == null) {
                        queryBuilder.append("'").append(UUID.randomUUID()).append("'");
                        queryBuilder.append(",");
                    }
                    addComma = false;
                    for (Map.Entry<String, Object> entry : attributeMap.entrySet()) {
                        if (addComma) {
                            queryBuilder.append(",");
                        }
                        queryBuilder.append("'").append(entry.getValue()).append("'");
                        addComma = true;
                    }
                    queryBuilder.append(")");
                    session.execute(queryBuilder.toString());


                }
                // end of customized code

            } catch (Throwable t) {
                if (cassandraClusterCache != null) {
                    cassandraClusterCache.remove(outputEventAdaptorConfiguration);
                }
                log.error("Cannot connect to Cassandra: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void testConnection(OutputEventAdaptorConfiguration outputEventAdaptorConfiguration, int i) {
        Map<String, String> properties = outputEventAdaptorConfiguration.getOutputProperties();

        String username = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_USER_NAME);
        String password = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PASSWORD);
        String cassandraHost = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_HOSTNAME);
        String cassandraPort = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_PORT);
        String clusterName = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_CLUSTER_NAME);
//        String keyspace = properties.get(CassandraExtendedEventAdaptorConstants.ADAPTOR_CASSANDRA_KEY_SPACE_NAME);


        try {
            Cluster.Builder clusterBuilder = Cluster.builder()
                    .addContactPoint(cassandraHost);
            if (cassandraPort != null && cassandraPort.length() > 0) {
                clusterBuilder.withPort(Integer.parseInt(cassandraPort));
            }
            clusterBuilder.withClusterName(clusterName);
            if (username != null && username.length() > 0) {
                clusterBuilder.withCredentials(username, password);
            }
            Cluster cluster = clusterBuilder.build();
//            Session session = cluster.connect(keyspace);
//            session.close();
            cluster.close();

        } catch (Exception ex) {
            throw new OutputEventAdaptorEventProcessingException("Couldn't connect to Cassandra cluster");

        }

    }


    @Override
    public void removeConnectionInfo(
            OutputEventAdaptorMessageConfiguration outputEventAdaptorMessageConfiguration,
            OutputEventAdaptorConfiguration outputEventAdaptorConfiguration, int tenantId) {
        ConcurrentHashMap<OutputEventAdaptorConfiguration, EventAdaptorInfo> cassandraClusterCache = tenantedCassandraClusterCache.get(tenantId);
        if (cassandraClusterCache != null) {
            cassandraClusterCache.remove(outputEventAdaptorConfiguration);
        }
    }

    class MessageInfo {
        private Session session;
        //        private BasicColumnFamilyDefinition columnFamilyDefinition;
        private List<String> columnNames = new ArrayList<String>();
        private boolean isInsertOrUpdate;
        private ArrayList<String> keyColumns;
        private String primaryKey = null;
        private boolean cfInitialized = false;


        MessageInfo() {
        }

        public Session getSession() {
            return session;
        }

        public void setSession(Session session) {
            this.session = session;
        }

        public boolean isCfInitialized() {
            return cfInitialized;
        }

        public void setCfInitialized(boolean cfInitialized) {
            this.cfInitialized = cfInitialized;
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }


        public List<String> getColumnNames() {
            return columnNames;
        }

        public boolean isInsertOrUpdate() {
            return isInsertOrUpdate;
        }

        public void setInsertOrUpdate(boolean insertOrUpdate) {
            isInsertOrUpdate = insertOrUpdate;
        }

        public ArrayList<String> getKeyColumns() {
            return keyColumns;
        }

        public void setKeyColumns(ArrayList<String> keyColumns) {
            this.keyColumns = keyColumns;
        }
    }

    class EventAdaptorInfo {
        private Cluster cluster;
        private boolean indexAllColumns;
        private ConcurrentHashMap<OutputEventAdaptorMessageConfiguration, MessageInfo> messageInfoMap = new ConcurrentHashMap<OutputEventAdaptorMessageConfiguration, MessageInfo>();

        EventAdaptorInfo(Cluster cluster, boolean indexAllColumns) {
            this.cluster = cluster;
            this.indexAllColumns = indexAllColumns;
        }

        public Cluster getCluster() {
            return cluster;
        }

        public ConcurrentHashMap<OutputEventAdaptorMessageConfiguration, MessageInfo> getMessageInfoMap() {
            return messageInfoMap;
        }

        public boolean isIndexAllColumns() {
            return indexAllColumns;
        }
    }
}
