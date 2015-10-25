/*
 * Copyright (c) 2015, Rajeev Sampath <rajeevs.net>
 *
 *  rajeevs.net. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.streamdef.util;

import org.apache.axiom.om.*;

import javax.xml.namespace.QName;
import java.io.*;
import java.util.Iterator;

public class StreamDefConverter {

    public static final String SM_CONF = "stream-definitions.xml";

    public static void main(String[] args) throws IOException {
        StreamDefConverter streamDefConverter = new StreamDefConverter();
        if (args.length < 1) {
            System.out.println("Please give the input file path as the first argument.");
            return;
        }
        String filePath = args[0];
        File f = new File(filePath);
        String convertedStreamConfig = streamDefConverter.convertTo310Format(f);
        streamDefConverter.writeToFile(SM_CONF, convertedStreamConfig);
    }



    public void writeToFile(String filePath, String streamDefXml) {
        try {
            /* save contents to .xml file */
            BufferedWriter out = new BufferedWriter(new FileWriter(filePath));
            String xmlContent = XmlFormatter.format(streamDefXml);
            out.write(streamDefXml);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public String convertTo310Format(File file) throws IOException, OMException {
        InputStream in = new FileInputStream(file);
        OMElement root = OMXMLBuilderFactory.createOMBuilder(in).getDocumentElement();

        Iterator it = root.getChildrenWithName(new QName("streamDefinition"));
        StringBuilder builder = new StringBuilder("");

        builder.append("<streamDefinitions xmlns=\"http://wso2.org/carbon/databridge\">\n");
        while (it.hasNext()) {
            OMElement om = (OMElement) it.next();
            String streamDefName = om.getAttributeValue(new QName("name"));
            String streamDefVersion = om.getAttributeValue(new QName("version"));

            builder.append("\n<streamDefinition>\n");
            builder.append("{");
            builder.append("\n");
            builder.append("\"name\":\"").append(streamDefName).append("\",\n");
            builder.append("\"version\":\"").append(streamDefVersion).append("\",\n");

            Iterator metaIterator = om.getChildrenWithName(new QName("metaData"));
            boolean appendComma = false;

            if (metaIterator.hasNext()) {
                builder.append("\"metaData\":[\n");

                OMElement meta = (OMElement) metaIterator.next();
                Iterator children = meta.getChildElements();
                convertAttributes(children, builder);

                builder.append("\n]");
                appendComma = true;
            }

            Iterator correlationIterator = om.getChildrenWithName(new QName("correlationData"));

            if (correlationIterator.hasNext()) {
                if (appendComma) {
                    builder.append(",\n");
                }
                appendComma = false;
                builder.append("\"correlationData\":[\n");

                OMElement correlation = (OMElement) correlationIterator.next();
                Iterator children = correlation.getChildElements();
                convertAttributes(children, builder);

                builder.append("\n]");
                appendComma = true;
            }


            Iterator payloadIterator = om.getChildrenWithName(new QName("payloadData"));

            if (payloadIterator.hasNext()) {
                if (appendComma) {
                    builder.append(",\n");
                }
                appendComma = false;

                builder.append("\"payloadData\":[\n");

                OMElement payload = (OMElement) payloadIterator.next();
                Iterator children = payload.getChildElements();
                convertAttributes(children, builder);
                builder.append("\n]");
            }

            builder.append("\n}");
            builder.append("\n</streamDefinition>");

        }

        builder.append("\n</streamDefinitions>");
        System.out.println(builder.toString());

        in.close();
        return builder.toString();
    }

    private void convertAttributes(Iterator attributes, StringBuilder builder) {
        boolean appendComma = false;
        while (attributes.hasNext()) {
            OMElement child = (OMElement) attributes.next();
            String name = child.getAttributeValue(new QName("name"));
            String type = child.getAttributeValue(new QName("type"));

            if (appendComma) {
                builder.append(",\n");
            }
            builder.append("{\"name\":\"").append(name).append("\",\"type\":\"").append(type).append("\"}");
            appendComma = true;
        }
    }



}
