/*
This is a controller providing some endpoints useful for development and testing.
To use it, simply rename and remove the '.dev' extension
*/

// eslint-disable-next-line max-classes-per-file
import { Controller, Logger, Post, Body, Param, Query, HttpException, HttpStatus } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { plainToClass } from 'class-transformer';
import { InjectRedis } from '@liaoliaots/nestjs-redis';
import Redis from 'ioredis';
import { GraphUpdateJobDto } from './interfaces/graph-update-job.dto';
import { ReconnectionGraphService } from './processor/reconnection-graph.service';
import { BlockchainScannerService, LAST_SEEN_BLOCK_NUMBER_KEY } from './blockchain-scanner.service';

@Controller('reconnection-service/dev')
export class DevelopmentController {
  private readonly logger: Logger;

  constructor(
    @InjectQueue('graphUpdateQueue') private graphUpdateQueue: Queue,
    @InjectRedis() private cacheManager: Redis,
    private graphService: ReconnectionGraphService,
    private scannerService: BlockchainScannerService,
  ) {
    this.logger = new Logger(this.constructor.name);

    this.graphUpdateQueue.on('paused', () => this.logger.debug('Queue is paused'));
    this.graphUpdateQueue.on('resumed', () => this.logger.debug('Queue has resumed'));
    this.graphUpdateQueue.on('error', (err) => this.logger.error(`Queue encountered an error: ${err}`));
    this.graphUpdateQueue.on('waiting', (job) =>
      this.logger.debug(`Queued job ${job.id}:
    ${JSON.stringify(job.data)}`),
    );
  }

  @Post('queue/clear')
  async clearQueue() {
    await this.graphUpdateQueue.drain();
    await this.graphUpdateQueue.obliterate();
    await this.graphUpdateQueue.resume();
  }

  @Post('queue/remove')
  async removeJob(@Param('jobId') jobId: string) {
    const job = await this.graphUpdateQueue.getJob(jobId);
    if (job) {
      await this.graphUpdateQueue.remove(jobId);
    } else {
      throw new HttpException('Job ID not found', HttpStatus.NOT_FOUND);
    }
  }

  @Post('queue/pause')
  pauseQueue() {
    this.graphUpdateQueue.pause();
  }

  @Post('queue/resume')
  resumeQueue() {
    this.graphUpdateQueue.resume();
  }

  @Post('queue/retry')
  async retryJob(@Param('jobId') jobId: string) {
    const job = await this.graphUpdateQueue.getJob(jobId);
    if (job) {
      job.retry();
    } else {
      throw new HttpException('Job ID not found', HttpStatus.NOT_FOUND);
    }
  }

  @Post('queue/update')
  async updateJob(@Body() payload: GraphUpdateJobDto) {
    const dto = plainToClass(GraphUpdateJobDto, payload);
    const { key: jobId, data } = dto.toGraphUpdateJob();
    let options: any = { jobId };
    if (typeof data.debugDisposition === 'object') {
      const debugOptions: any = data.debugDisposition;
      options = { ...options, ...debugOptions };
    }
    const job = await this.graphUpdateQueue.getJob(jobId);
    if (job) {
      await job.update(data);
      await job.retry();
    } else {
      throw new HttpException('Job ID not found', HttpStatus.NOT_FOUND);
    }
  }

  @Post('queue')
  async enqueueJob(@Body() payload: GraphUpdateJobDto) {
    const dto = plainToClass(GraphUpdateJobDto, payload);
    const { key: jobId, data } = dto.toGraphUpdateJob();
    let options: any = { jobId };
    if (typeof data.debugDisposition === 'object') {
      const debugData: any = data.debugDisposition;
      const { action, ...debugOptions } = debugData;
      options = { ...options, ...debugOptions };
    }
    return this.graphUpdateQueue.add(`graphUpdate:${data.dsnpId}`, data, options);
  }

  @Post('update/graph')
  updateGraph(@Body() payload: GraphUpdateJobDto) {
    this.graphService.updateUserGraph(payload.dsnpId, payload.providerId, true);
  }

  @Post('scan/:blockNumber')
  async scanChainFromBlock(@Param('blockNumber') blockNumber: string) {
    const block = BigInt(blockNumber) - 1n;
    await this.cacheManager.set(LAST_SEEN_BLOCK_NUMBER_KEY, block.toString());
    this.scannerService.scan();
  }

  @Post('scan')
  scanChain() {
    this.scannerService.scan();
  }
}
