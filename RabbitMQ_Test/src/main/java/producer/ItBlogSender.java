package producer;

import com.rabbitmq.client.*;

import java.util.*;

public class ItBlogSender {
    private static final String EXCHANGER_NAME = "blog_exchanger";

    public ItBlogSender(){
    }

    public static void main(String[] args) throws Exception {
        List<String> messages = new ArrayList<>(Arrays.asList(
                "PHP, PHP bla-bla-bla",
                "Java, Java bla-bla-bla",
                "C, C bla-bla-bla",
                "Ruby, Ruby bla-bla-bla")
        );

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.TOPIC);
            for (int i = 0; i < 100; i++) {
                String[] messageDetails = getMessageDetails(messages);
                String key = messageDetails[0];
                String message = messageDetails[1];
                channel.basicPublish(EXCHANGER_NAME, key, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + key + "':'" + message + "'");
                Thread.sleep(1000);
            }
        }
    }

    private static String[] getMessageDetails(List<String> messages) {
        int i = (int)(Math.random()*(messages.size()));
        return messages.get(i).split(",");
    }
}
