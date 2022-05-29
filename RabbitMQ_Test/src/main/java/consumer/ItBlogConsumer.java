package consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;

public class ItBlogConsumer {
    private static final String EXCHANGER_NAME = "blog_exchanger";

    public static void main(String[] argv) throws Exception {
        String key = setTopic();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.TOPIC);
        String queueName = channel.queueDeclare().getQueue();
        System.out.println("QUEUE NAME: " + queueName);
        channel.queueBind(queueName, EXCHANGER_NAME, key);
        System.out.println(" [*] Waiting for messages with routing key (" + key + "):");
        Runnable commandListener = new Runnable() {
            @Override
            public void run() {
                Scanner scanner = new Scanner(System.in);
                while (true){
                    System.out.println("Если хотите поменять тему, то введите команду change");
                    if(scanner.next().equals("change")){
                        try {
                            changeTopic(channel, queueName,key);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
        Thread t = new Thread(commandListener);
        t.start();
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

    }

    private static String setTopic() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Введите тему через команду set_topic/");
        String[] command = scanner.next().split("/");
        return command[1];
    }

    private static synchronized void changeTopic(Channel channel, String queueName, String key) throws IOException {
        channel.queueUnbind(queueName, EXCHANGER_NAME,key);
        key = setTopic();
        channel.queueBind(queueName,EXCHANGER_NAME, key);
    }

}
