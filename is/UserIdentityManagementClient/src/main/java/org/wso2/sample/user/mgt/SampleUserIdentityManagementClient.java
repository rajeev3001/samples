package org.wso2.sample.user.mgt;

import org.apache.axis2.client.Options;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ConfigurationContextFactory;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.http.HttpTransportProperties;
import org.wso2.carbon.identity.mgt.stub.UserIdentityManagementAdminServiceStub;
import org.wso2.carbon.identity.mgt.stub.dto.ChallengeQuestionDTO;
import org.wso2.carbon.identity.mgt.stub.dto.UserChallengesDTO;

import java.io.File;

/*
Demonstrates the use of user identity management admin service for setting/retrieving user challenges.
 */
public class SampleUserIdentityManagementClient {


    /*
    carbon url
     */
    private static String serverUrl = "https://localhost:9443/services/";

    /*
    credentials to access carbon
     */
    private static String username = "admin";
    private static String password = "admin";


    public static void main(String args[]) {


        /*
        setting the trust store that contains the certificates.
         */
        String trustStore = System.getProperty("user.dir") + File.separator +
                "src" + File.separator + "main" + File.separator +
                "resources" + File.separator + "wso2carbon.jks";
        System.setProperty("javax.net.ssl.trustStore", trustStore);
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");


        try {

        /*
        the configuration context contains axis2 environment information. we use this to create the axis2 client.
         */
            ConfigurationContext configContext =
                    ConfigurationContextFactory.createConfigurationContextFromFileSystem(null, null);

            // user identity management admin service url.
            String adminServiceEndpoint = serverUrl + "UserIdentityManagementAdminService";

            // stub and the service client
            UserIdentityManagementAdminServiceStub identityStub =
                    new UserIdentityManagementAdminServiceStub(configContext, adminServiceEndpoint);
            ServiceClient adminClient = identityStub._getServiceClient();
            Options adminClientOptions = adminClient.getOptions();


            /*
            if you already have an auth cookie, you can set it here.
             */
            adminClientOptions.setProperty(HTTPConstants.COOKIE_STRING, null);

            /*
             setting auth headers for authentication for carbon server
             */
            HttpTransportProperties.Authenticator auth = new HttpTransportProperties.Authenticator();
            auth.setUsername(username);
            auth.setPassword(password);
            auth.setPreemptiveAuthentication(true);
            adminClientOptions.setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
            adminClientOptions.setManageSession(true);


            // listing all available challenge questions.
            ChallengeQuestionDTO[] questions = identityStub.getAllChallengeQuestions();
            System.out.println("All Challenge questions:");
            for (ChallengeQuestionDTO dto : questions) {
                System.out.println(dto.getQuestion());
            }

            /*
            adding a new challenge question/answer for the user1, assuming there's a user already existing
            named user1
             */
            UserChallengesDTO[] newUserChallenges = new UserChallengesDTO[1];
            UserChallengesDTO newChallengeDto = new UserChallengesDTO();
            newChallengeDto.setOrder(0);
            newChallengeDto.setQuestion(questions[0].getQuestion());
            newChallengeDto.setId(questions[0].getQuestionSetId());
            newChallengeDto.setAnswer("colombo");
            newUserChallenges[0] = newChallengeDto;

            identityStub.setChallengeQuestionsOfUser("user1", newUserChallenges);

            /*
            listing existing challenges of user1
             */
            System.out.println("Existing challenges of user1:");
            UserChallengesDTO[] userChallenges = identityStub.getChallengeQuestionsOfUser("user1");
            for (UserChallengesDTO dto : userChallenges) {
                System.out.println(dto.getQuestion());
                System.out.println(dto.getAnswer());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}