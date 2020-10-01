/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.receptor_rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author christianpineda
 */
public class Receive {

    /**
     * @param args the command line arguments
     */
    private final static String QUEUE_NAME = "MAIN_QUEUE";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        // TODO code application logic here
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] A la espera de mensajes. Para salir pulse: CTRL+C");
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println(" [x] Recibido: '" + message + "'");
            doWork(message);
            System.out.println(" [x] Hecho!!! ");
        }
    }

    private static void doWork(String task) throws InterruptedException {
        Thread.sleep(8000);
    }

}
