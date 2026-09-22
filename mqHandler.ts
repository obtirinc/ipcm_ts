import amqp, { Channel, Connection, ConsumeMessage } from 'amqplib';

export class RabbitHandler {
  private rabbitmqUrl: string;
  private connection: Connection | null = null;
  private channel: Channel | null = null;

  constructor(rabbitmqUrl: string) {
    this.rabbitmqUrl = rabbitmqUrl;
  }

  async connect(): Promise<void> {
    this.connection = await amqp.connect(this.rabbitmqUrl);
    this.channel = await this.connection.createChannel();
  }

  async close(): Promise<void> {
    if (this.channel) {
      await this.channel.close();
      this.channel = null;
    }
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
  }

  async publishRequest(queueName: string, payload: unknown, responseChannel: string): Promise<void> {
    if (!this.channel) {
      throw new Error("RabbitMQ channel is not open.");
    }

    await this.channel.assertQueue(queueName, { durable: true });

    const messageBody = Buffer.from(
      JSON.stringify({
        response_channel: responseChannel,
        data: payload
      })
    );

    this.channel.sendToQueue(queueName, messageBody, { persistent: true });
  }

  async startConsumer(
    queueName: string,
    onMessageCallback: (msg: ConsumeMessage, channel: Channel) => Promise<void>
  ): Promise<void> {
    if (!this.channel) {
      throw new Error("RabbitMQ channel is not open.");
    }

    await this.channel.prefetch(1);
    await this.channel.assertQueue(queueName, { durable: true });

    const currentChannel = this.channel;
    await this.channel.consume(queueName, async (msg: ConsumeMessage | null) => {
      if (msg !== null) {
        await onMessageCallback(msg, currentChannel);
      }
    });
  }
}