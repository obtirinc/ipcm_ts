import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

export class RedisHandler {
  private redisUrl: string;
  private client: Redis | null = null;

  constructor(redisUrl: string) {
    this.redisUrl = redisUrl;
  }

  async connect(): Promise<void> {
    this.client = new Redis(this.redisUrl, { lazyConnect: true });
    await this.client.connect();
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.quit();
      this.client = null;
    }
  }

  generateChannelId(): string {
    return `response_channel:${uuidv4()}`;
  }

  async waitForResponse<T = unknown>(channelId: string, timeoutSeconds: number): Promise<T> {
    if (!this.client) {
      throw new Error("Redis client is not connected. Call connect() first.");
    }

    // In Redis, a client in subscriber mode cannot execute normal commands.
    // Duplicate the existing connection pool for subscribing.
    const subscriber = this.client.duplicate();
    await subscriber.connect();

    return new Promise<T>((resolve, reject) => {
      let timer: NodeJS.Timeout | null = null;

      const cleanup = async (): Promise<void> => {
        if (timer) clearTimeout(timer);
        try {
          await subscriber.unsubscribe(channelId);
          await subscriber.quit();
        } catch (_) {
          // Ignore cleanup errors
        }
      };

      timer = setTimeout(async () => {
        await cleanup();
        reject(new Error(`No response received on ${channelId} within ${timeoutSeconds}s`));
      }, timeoutSeconds * 1000);

      subscriber.subscribe(channelId, (err) => {
        if (err) {
          cleanup();
          return reject(err);
        }
      });

      subscriber.on('message', async (channel: string, message: string) => {
        if (channel === channelId) {
          await cleanup();
          try {
            resolve(JSON.parse(message) as T);
          } catch (parseError) {
            reject(parseError);
          }
        }
      });
    });
  }

  async publishResponse(channelId: string, data: unknown): Promise<void> {
    if (!this.client) {
      throw new Error("Redis client is not connected.");
    }
    await this.client.publish(channelId, JSON.stringify(data));
  }
}