/* eslint-disable max-classes-per-file */
/* eslint-disable class-methods-use-this */
/*
https://docs.nestjs.com/controllers#controllers
*/

import { Body, Controller, Get, HttpException, HttpStatus, Logger, Param, Post } from '@nestjs/common';
import fs from 'fs';

class ProviderRequestDto {
  pageNumber: number;

  pageSize: number;
}

@Controller()
export class MockWebhookController {
  private logger: Logger;

  private healthResponse: HttpStatus = HttpStatus.OK;

  constructor() {
    this.logger = new Logger(this.constructor.name);
  }

  @Get('/api/v1.0.0/health')
  public health() {
    if (this.healthResponse !== HttpStatus.OK) {
      throw new HttpException('Unhealthy', this.healthResponse);
    }
    return this.healthResponse;
  }

  @Post('/api/v1.0.0/health/toggleResponse')
  public toggleHealth() {
    this.healthResponse = this.healthResponse === HttpStatus.OK ? HttpStatus.GONE : HttpStatus.OK;
  }

  @Get('/api/v1.0.0/connections/:dsnpId')
  public getConnections(@Param('dsnpId') dsnpId: string, @Body() params: ProviderRequestDto) {
    if (this.healthResponse !== HttpStatus.OK) {
      this.logger.log(`/connections returning ${this.healthResponse}`);
      throw new HttpException('Bad endpoint', this.healthResponse);
    }

    const { pageNumber } = params;

    let filename: string = '';
    if (!pageNumber || Number(pageNumber) === 1) {
      if (fs.existsSync(`./responses/response.${dsnpId}.1.json`)) {
        filename = `./responses/response.${dsnpId}.1.json`;
      } else if (fs.existsSync(`./responses/response.${dsnpId}.json`)) {
        filename = `./responses/response.${dsnpId}.json`;
      }
    } else if (fs.existsSync(`./responses/response.${dsnpId}.${pageNumber}.json`)) {
      filename = `./responses/response.${dsnpId}.${pageNumber}.json`;
    } else if (fs.existsSync('./responses/response.default.json')) {
      filename = './responses/response.default.json';
    }

    if (filename) {
      const content = fs.readFileSync(filename);
      const obj = JSON.parse(content.toString());
      obj.dsnpId = dsnpId;
      return obj;
    }

    return HttpStatus.NO_CONTENT;
  }
}
