import { RabbitHandler } from './mqHandler';
import { RedisHandler } from './cachePubSub';
import { Channel, ConsumeMessage } from 'amqplib';

export class RemoteServiceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RemoteServiceError';
    Object.setPrototypeOf(this, RemoteServiceError.prototype);
  }
}

export class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
    Object.setPrototypeOf(this, TimeoutError.prototype);
  }
}

export interface IPCSuccessResponse<T = unknown> {
  status: 'success';
  data: T;
}

export interface IPCErrorResponse {
  status: 'error';
  message: string;
}

export type IPCResponse<T = unknown> = IPCSuccessResponse<T> | IPCErrorResponse;

export interface QueueMessage<T = unknown> {
  response_channel: string;
  data: T;
}

export class IPCMessenger {
  public mq: RabbitHandler;
  public redis: RedisHandler;

  constructor(rabbitmqUrl: string, redisUrl: string) {
    this.mq = new RabbitHandler(rabbitmqUrl);
    this.redis = new RedisHandler(redisUrl);
  }

  async connect(): Promise<void> {
    await Promise.all([this.mq.connect(), this.redis.connect()]);
  }

  async close(): Promise<void> {
    await Promise.all([this.mq.close(), this.redis.close()]);
  }

  async sendRequest<TResponse = unknown, TPayload = unknown>(
    queueName: string,
    payload: TPayload,
    timeout: number = 10
  ): Promise<TResponse> {
    const channelId = this.redis.generateChannelId();

    await this.mq.publishRequest(queueName, payload, channelId);

    try {
      const response = await this.redis.waitForResponse<IPCResponse<TResponse>>(channelId, timeout);

      if (response.status === 'error') {
        throw new RemoteServiceError(response.message);
      }
      return response.data;
    } catch (err: unknown) {
      if (err instanceof Error && err.message.includes('within')) {
        throw new TimeoutError(`Request to queue '${queueName}' timed out.`);
      }
      throw err;
    }
  }

  async startListening<TInput = unknown, TOutput = unknown>(
    queueName: string,
    processCallback: (data: TInput) => Promise<TOutput> | TOutput
  ): Promise<void> {
    const onMessage = async (msg: ConsumeMessage, channel: Channel): Promise<void> => {
      let payload: IPCResponse<TOutput>;
      let respChannel: string | null = null;

      try {
        const body = JSON.parse(msg.content.toString()) as QueueMessage<TInput>;
        respChannel = body.response_channel;

        const result = await processCallback(body.data);
        payload = { status: 'success', data: result };
      } catch (e: unknown) {
        const err = e as Error;
        payload = { status: 'error', message: `${err.name || 'Error'}: ${err.message}` };
      }

      if (respChannel) {
        await this.redis.publishResponse(respChannel, payload);
      }

      // Acknowledge the message once processing and publishing complete
      channel.ack(msg);
    };

    await this.mq.startConsumer(queueName, onMessage);
  }
}