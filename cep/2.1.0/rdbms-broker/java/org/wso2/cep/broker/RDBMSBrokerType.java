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

package org.wso2.cep.broker;

import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.broker.core.*;
import org.wso2.carbon.broker.core.exception.BrokerEventProcessingException;
import org.wso2.carbon.databridge.commons.Attribute;
import org.wso2.carbon.databridge.commons.AttributeType;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;

public class RDBMSBrokerType implements BrokerType {

    private static final Log log = LogFactory.getLog(RDBMSBrokerType.class);
    private BrokerTypeDto brokerTypeDto = null;
    private String urlPrefix;
    private String driverClassName;

    // database name vs connection
    private ConcurrentHashMap<BrokerConfiguration, ConcurrentHashMap<String, Connection>> jdbcConnections = new ConcurrentHashMap<BrokerConfiguration, ConcurrentHashMap<String, Connection>>();

    // broker conf to topic name vs table info
    private ConcurrentHashMap<BrokerConfiguration, ConcurrentHashMap<String, TableInfo>> tables = new ConcurrentHashMap<BrokerConfiguration, ConcurrentHashMap<String, TableInfo>>();

    private static final String BROKER_TYPE_RDBMS = "RDBMS";
    private static final String BROKER_CONF_RDBMS_PROPERTY_USER_NAME_REFERENCE = "UserName";
    private static final String BROKER_CONF_RDBMS_PROPERTY_PASSWORD_REFERENCE = "Password";
    private static final String BROKER_CONF_RDBMS_PROPERTY_HOSTNAME_REFERENCE = "Hostname";
    private static final String BROKER_CONF_RDBMS_PROPERTY_PORT_REFERENCE = "Port";

    private static final String BROKER_CONF_RDBMS_PROPERTY_DISPLAY_USER_NAME_REFERENCE = "rdbms.broker.property.display.user.name";
    private static final String BROKER_CONF_RDBMS_PROPERTY_DISPLAY_PASSWORD_REFERENCE = "rdbms.broker.property.display.password";
    private static final String BROKER_CONF_RDBMS_PROPERTY_DISPLAY_HOSTNAME_REFERENCE = "rdbms.broker.property.display.hostname";
    private static final String BROKER_CONF_RDBMS_PROPERTY_DISPLAY_PORT_REFERENCE = "rdbms.broker.property.display.port";

    private static final String BROKER_CONF_RDBMS_DRIVER_CLASS_REFERENCE = "rdbms.broker.property.driver.class";
    private static final String BROKER_CONF_RDBMS_URL_PREFIX_REFERENCE = "rdbms.broker.property.url.prefix";

    private static RDBMSBrokerType instance;

    private RDBMSBrokerType() {
        this.brokerTypeDto = new BrokerTypeDto();
        this.brokerTypeDto.setName(BROKER_TYPE_RDBMS);

        ResourceBundle configResourceBundle = ResourceBundle.getBundle(
                "org.wso2.cep.broker.config", Locale.getDefault());
        driverClassName = configResourceBundle.getString(BROKER_CONF_RDBMS_DRIVER_CLASS_REFERENCE);
        urlPrefix = configResourceBundle.getString(BROKER_CONF_RDBMS_URL_PREFIX_REFERENCE);

        ResourceBundle resourceBundle = ResourceBundle.getBundle(
                "org.wso2.cep.broker.resources", Locale.getDefault());

        Property hostName = new Property(BROKER_CONF_RDBMS_PROPERTY_HOSTNAME_REFERENCE);
        hostName.setRequired(true);
        hostName.setDisplayName(resourceBundle.getString(
                BROKER_CONF_RDBMS_PROPERTY_DISPLAY_HOSTNAME_REFERENCE));
        getBrokerTypeDto().addProperty(hostName);

        Property port = new Property(BROKER_CONF_RDBMS_PROPERTY_PORT_REFERENCE);
        port.setRequired(true);
        port.setDisplayName(resourceBundle.getString(
                BROKER_CONF_RDBMS_PROPERTY_DISPLAY_PORT_REFERENCE));
        getBrokerTypeDto().addProperty(port);

        Property username = new Property(BROKER_CONF_RDBMS_PROPERTY_USER_NAME_REFERENCE);
        username.setRequired(true);
        username.setDisplayName(resourceBundle.getString(
                BROKER_CONF_RDBMS_PROPERTY_DISPLAY_USER_NAME_REFERENCE));
        getBrokerTypeDto().addProperty(username);


        Property password = new Property(BROKER_CONF_RDBMS_PROPERTY_PASSWORD_REFERENCE);
        password.setRequired(true);
        password.setSecured(true);
        password.setDisplayName(resourceBundle.getString(
                BROKER_CONF_RDBMS_PROPERTY_DISPLAY_PASSWORD_REFERENCE));
        getBrokerTypeDto().addProperty(password);


    }

    public static synchronized RDBMSBrokerType getInstance() {
        if (instance == null) {
            instance = new RDBMSBrokerType();
        }
        return instance;
    }

    @Override
    public BrokerTypeDto getBrokerTypeDto() {
        return brokerTypeDto;
    }

    @Override
    public String subscribe(String s, BrokerListener brokerListener, BrokerConfiguration brokerConfiguration, AxisConfiguration axisConfiguration) throws BrokerEventProcessingException {
        throw new BrokerEventProcessingException("Subscriptions not supported.");
    }

    @Override
    public void publish(String topic, Object message, BrokerConfiguration brokerConfiguration) throws BrokerEventProcessingException {
        try {
            String[] topicItems = topic.split("\\.");
            String databaseName = topicItems[0].trim();
            String tableName = topicItems[1].trim();

            ConcurrentHashMap<String, Connection> connectionMap = jdbcConnections.get(brokerConfiguration);

            if (connectionMap == null) {
                connectionMap = new ConcurrentHashMap<String, Connection>();
                jdbcConnections.put(brokerConfiguration, connectionMap);
            }
            Connection connection  = connectionMap.get(databaseName);

            if (connection == null) {
                Class.forName(driverClassName);
                connection = DriverManager.getConnection(urlPrefix + "://" + brokerConfiguration.getProperties().get(BROKER_CONF_RDBMS_PROPERTY_HOSTNAME_REFERENCE) + ":" + brokerConfiguration.getProperties().get(BROKER_CONF_RDBMS_PROPERTY_PORT_REFERENCE) + "/" + databaseName + "?user=" +
                        brokerConfiguration.getProperties().get(BROKER_CONF_RDBMS_PROPERTY_USER_NAME_REFERENCE) + "&password=" + brokerConfiguration.getProperties().get(BROKER_CONF_RDBMS_PROPERTY_PASSWORD_REFERENCE));
                connectionMap.put(databaseName, connection);
            }

            ConcurrentHashMap<String, TableInfo> tableInfoMap = tables.get(brokerConfiguration);
            TableInfo tableInfo;
            if (tableInfoMap == null || tableInfoMap.get(topic) == null) {
                tableInfo = initializeTableInfo(databaseName, tableName, message, brokerConfiguration, connection);
                if (tableInfoMap == null) {
                    tableInfoMap = new ConcurrentHashMap<String, TableInfo>();
                    tables.put(brokerConfiguration, tableInfoMap);
                }
            } else {
                tableInfo = tableInfoMap.get(topic);
            }

            PreparedStatement preparedStatement = tableInfo.getPreparedStatement();
            Map<String, Object> map = (Map<String, Object>) message;
            Attribute attribute;
            for (int i = 0; i < tableInfo.getColumnOrder().size(); i++) {
                attribute = tableInfo.getColumnOrder().get(i);
                switch (attribute.getType()) {
                    case INT:
                        preparedStatement.setInt(i + 1, (Integer) map.get(attribute.getName()));
                        break;
                    case LONG:
                        preparedStatement.setLong(i + 1, (Long) map.get(attribute.getName()));
                        break;
                    case FLOAT:
                        preparedStatement.setFloat(i + 1, (Float) map.get(attribute.getName()));
                        break;
                    case DOUBLE:
                        preparedStatement.setDouble(i + 1, (Double) map.get(attribute.getName()));
                        break;
                    case STRING:
                        preparedStatement.setString(i + 1, (String) map.get(attribute.getName()));
                        break;
                    case BOOL:
                        preparedStatement.setBoolean(i + 1, (Boolean) map.get(attribute.getName()));
                        break;
                }
            }
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            log.error(e);
        } catch (ClassNotFoundException e) {
            log.error(e);
        }
    }

    private TableInfo initializeTableInfo(String databaseName, String tableName, Object message, BrokerConfiguration brokerConfiguration, Connection connection) throws SQLException {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(tableName);

        // create the table.
        StringBuilder stringBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        stringBuilder.append(tableName);
        stringBuilder.append(" (");
        boolean appendComma = false;
        for (Map.Entry<String, Object> entry : (((Map<String, Object>) message).entrySet())) {
            if (appendComma) {
                stringBuilder.append(",");
            } else {
                appendComma = true;
            }
            stringBuilder.append(entry.getKey()).append("  ");
            if (entry.getValue() instanceof Integer) {
                stringBuilder.append("INT");
            } else if (entry.getValue() instanceof Long) {
                stringBuilder.append("BIGINT");
            } else if (entry.getValue() instanceof Float) {
                stringBuilder.append("FLOAT");
            } else if (entry.getValue() instanceof Double) {
                stringBuilder.append("DOUBLE");
            } else if (entry.getValue() instanceof String) {
                stringBuilder.append("VARCHAR(255)");
            } else if (entry.getValue() instanceof Boolean) {
                stringBuilder.append("BOOL");
            }
        }
        stringBuilder.append(")");
        Statement statement = connection.createStatement();
        statement.executeUpdate(stringBuilder.toString());
        statement.close();

        ArrayList<Attribute> tableColumnList = new ArrayList<Attribute>();
        stringBuilder = new StringBuilder("INSERT INTO ");
        stringBuilder.append(tableName);
        stringBuilder.append(" ( ");
        StringBuilder values = new StringBuilder("");

        appendComma = false;
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet rs = databaseMetaData.getColumns(null, null, databaseName + "." + tableName, null);
        while (rs.next()) {
            AttributeType type = null;
            int colType = rs.getInt("DATA_TYPE");
            switch (colType) {
                case Types.VARCHAR:
                    type = AttributeType.STRING;
                    break;
                case Types.INTEGER:
                    type = AttributeType.INT;
                    break;
                case Types.BIGINT:
                    type = AttributeType.LONG;
                    break;
                case Types.DOUBLE:
                    type = AttributeType.DOUBLE;
                    break;
                case Types.FLOAT:
                    type = AttributeType.FLOAT;
                    break;
                case Types.BOOLEAN:
                    type = AttributeType.BOOL;
                    break;

            }
            Attribute attribute = new Attribute(rs.getString("COLUMN_NAME"), type);
            tableColumnList.add(attribute);

            if (appendComma) {
                stringBuilder.append(",");
                values.append(",");
            } else {
                appendComma = true;
            }
            stringBuilder.append(attribute.getName());
            values.append("?");

        }

        stringBuilder.append(") VALUES (");
        stringBuilder.append(values);
        stringBuilder.append(")");
        tableInfo.setColumnOrder(tableColumnList);
        tableInfo.setPreparedStatement(connection.prepareStatement(stringBuilder.toString()));
        return tableInfo;
    }

    @Override
    public void testConnection(BrokerConfiguration brokerConfiguration) throws BrokerEventProcessingException {

    }

    @Override
    public void unsubscribe(String s, BrokerConfiguration brokerConfiguration, AxisConfiguration axisConfiguration, String s2) throws BrokerEventProcessingException {
        throw new BrokerEventProcessingException("Subscribing not supported.");
    }

    private class TableInfo {
        private String tableName;
        private PreparedStatement preparedStatement;
        private ArrayList<Attribute> columnOrder;

        public PreparedStatement getPreparedStatement() {
            return preparedStatement;
        }

        public void setPreparedStatement(PreparedStatement preparedStatement) {
            this.preparedStatement = preparedStatement;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public ArrayList<Attribute> getColumnOrder() {
            return columnOrder;
        }

        public void setColumnOrder(ArrayList<Attribute> columnOrder) {
            this.columnOrder = columnOrder;
        }
    }


}
