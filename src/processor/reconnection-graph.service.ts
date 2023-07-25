import axios, { AxiosError, AxiosInstance } from 'axios';
import { Injectable, Logger } from '@nestjs/common';
import { DelegatorId, ItemizedStoragePageResponse, ItemizedStorageResponse, MessageSourceId, PaginatedStorageResponse, ProviderId } from '@frequency-chain/api-augment/interfaces';
import {
  ImportBundleBuilder,
  Config,
  ConnectAction,
  Connection,
  ConnectionType,
  DsnpKeys,
  GraphKeyType,
  ImportBundle,
  KeyData,
  PrivacyType,
  Update,
  GraphKeyPair,
} from '@dsnp/graph-sdk';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { SubmittableExtrinsic } from '@polkadot/api-base/types';
import { AnyNumber, ISubmittableResult } from '@polkadot/types/types';
import { number } from 'joi';
import { SkipTransitiveGraphs, createGraphUpdateJob } from '../interfaces/graph-update-job.interface';
import { BlockchainService } from '../blockchain/blockchain.service';
import { createKeys } from '../blockchain/create-keys';
import { GraphKeyPair as ProviderKeyPair, KeyType, ProviderGraph } from '../interfaces/provider-graph.interface';
import { GraphStateManager } from '../graph/graph-state-manager';
import { ConfigService } from '../config/config.service';
import { ParsedEventResult } from '../blockchain/extrinsic';
import { KeyringPair } from '@polkadot/keyring/types';

@Injectable()
export class ReconnectionGraphService {
  private logger: Logger;

  constructor(
    private configService: ConfigService,
    private graphStateManager: GraphStateManager,
    @InjectQueue('graphUpdateQueue') private graphUpdateQueue: Queue,
    private blockchainService: BlockchainService,
  ) {
    this.logger = new Logger(ReconnectionGraphService.name);
  }

  public get capacityBatchLimit(): number {
    return this.blockchainService.api.consts.frequencyTxPayment.maximumCapacityBatchLength.toNumber();
  }

  public async updateUserGraph(dsnpUserStr: string, providerStr: string, updateConnections: boolean): Promise<void> {
    this.logger.debug(`Updating graph for user ${dsnpUserStr}, provider ${providerStr}`);
    const dsnpUserId: MessageSourceId = this.blockchainService.api.registry.createType('MessageSourceId', dsnpUserStr);
    const providerId: ProviderId = this.blockchainService.api.registry.createType('ProviderId', providerStr);
    const { key: jobId_nt, data: data_nt } = createGraphUpdateJob(dsnpUserId, providerId, SkipTransitiveGraphs);

    let graphConnections: ProviderGraph[] = [];
    let graphKeyPairs: ProviderKeyPair[] = [];
    try {
      [graphConnections, graphKeyPairs] = await this.getUserGraphFromProvider(dsnpUserId, providerId);
    } catch (e) {
      this.logger.error(`Error getting user graph from provider: ${e}`);
      throw e;
    }

    try {
      // get the user's DSNP Graph from the blockchain and form import bundles
      // import bundles are used to import the user's DSNP Graph into the graph SDK
      await this.importBundles(dsnpUserId, graphKeyPairs);
      // using graphConnections form Action[] and update the user's DSNP Graph
      const actions: ConnectAction[] = await this.formConnections(dsnpUserId, providerId, updateConnections, graphConnections);
      try {
        await this.graphStateManager.applyActions(actions);
      } catch (e: any) {
        const errMessage = e instanceof Error ? e.message : ""
        if (errMessage.includes('already exists')) {
          this.logger.warn(`Error applying actions: ${e}`);
        } else {
          throw e;
        }
      }

      let exportedUpdates = await this.graphStateManager.exportGraphUpdates();

      const providerKeys = createKeys(this.configService.getProviderAccountSeedPhrase());
      const mapUserIdToUpdates = new Map<MessageSourceId, Update[]>();
      /// Note: for now exporting updates for a single user
      exportedUpdates = exportedUpdates.filter((update) => update.ownerDsnpUserId === dsnpUserStr);

      // loop over exportUpdates and collect Updates vs userId
      exportedUpdates.forEach((bundle) => {
        const ownerMsaId: MessageSourceId = this.blockchainService.api.registry.createType('MessageSourceId', bundle.ownerDsnpUserId);
        if (mapUserIdToUpdates.has(ownerMsaId)) {
          const updates = mapUserIdToUpdates.get(ownerMsaId) || [];
          updates.push(bundle);
          mapUserIdToUpdates.set(ownerMsaId, updates);
        } else {
          mapUserIdToUpdates.set(ownerMsaId, [bundle]);
        }
      });

      for (const [ownerMsaId, updates] of mapUserIdToUpdates.entries()) {
        let batch: SubmittableExtrinsic<'rxjs', ISubmittableResult>[] = [];
        let batchItemCount = 0;
        let batchCount = 0;
        const batches: SubmittableExtrinsic<'rxjs', ISubmittableResult>[][] = [];
        updates.forEach((bundle) => {
          switch (bundle.type) {
            case 'PersistPage':
              batch.push(
                this.blockchainService.createExtrinsicCall(
                  { pallet: 'statefulStorage', extrinsic: 'upsertPage' },
                  ownerMsaId,
                  bundle.schemaId,
                  bundle.pageId,
                  bundle.prevHash,
                  Array.from(Array.prototype.slice.call(bundle.payload)),
                ),
              );
              batchItemCount++;
              // If the batch size exceeds the capacityBatchLimit, send the batch to the chain
              if (batchItemCount === this.capacityBatchLimit) {
                // Reset the batch and count for the next batch
                batches.push(batch);
                batchCount++;
                batch = [];
                batchItemCount = 0;
              }
              break;

            default:
              break;
          }
        });

        if (batch.length > 0) {
          batches.push(batch);
        }
        await this.sendAndProcessChainEvents(ownerMsaId, providerId, providerKeys, graphKeyPairs, batches);

        // On successful export to chain, re-import the user's DSNP Graph from the blockchain and form import bundles
        // import bundles are used to import the user's DSNP Graph into the graph SDK
        // check if user graph exists in the graph SDK else queue a graph update job
        const reImported = await this.importBundles(dsnpUserId, graphKeyPairs);
        if (reImported) {
          const userGraphExists = await this.graphStateManager.graphContainsUser(dsnpUserId.toString());
          if (!userGraphExists) {
            throw new Error(`User graph does not exist for ${dsnpUserId.toString()}`);
          }
        } else {
          throw new Error(`Error re-importing bundles for ${dsnpUserId.toString()}`);
        }
      }
    } catch (err) {
      if (updateConnections) {
        this.graphUpdateQueue.add('graphUpdate', data_nt, { jobId: jobId_nt });
      } else {
        this.logger.error(err);
        throw err;
      }
    }
  }

  async getUserGraphFromProvider(dsnpUserId: MessageSourceId | string, providerId: ProviderId | string): Promise<any> {
    const headers = {
      Authorization: 'Bearer <access_token>', // Replace with your actual access token if required
    };
    const baseUrl = this.configService.providerBaseUrl(providerId);

    const params = {
      pageNumber: 1,
      pageSize: 10, // This likely should be increased for production values
    };

    const providerAPI: AxiosInstance = axios.create({
      baseURL: baseUrl.toString(),
      headers,
    });

    const allConnections: ProviderGraph[] = [];
    const keyPairs: GraphKeyPair[] = [];
    try {
      let hasNextPage = true;
      while (hasNextPage) {
        // eslint-disable-next-line no-await-in-loop
        const response = await providerAPI.get(`/api/v1.0.0/connections/${dsnpUserId.toString()}`, { params });
        if (response.status !== 200) {
          throw new Error(`Bad status ${response.status} (${response.statusText} from Provider web hook.)`);
        }
        if (!response.data || !response.data.connections) {
          throw new Error(`No connections found for ${dsnpUserId.toString()}`);
        }
        
        if(response.data.dsnpId !== dsnpUserId.toString()) {
          throw new Error(`DSNP ID mismatch in response for ${dsnpUserId.toString()}`);
        }

        const { data }: { data: ProviderGraph[] } = response.data.connections;
        allConnections.push(...data);

        const { graphKeyPairs }: { graphKeyPairs: GraphKeyPair[] } = response.data.graphKeyPairs;
        if (graphKeyPairs) {
          keyPairs.push(...graphKeyPairs);
        }

        const { pagination } = response.data.connections;
        if (pagination && pagination.pageCount && pagination.pageCount > params.pageNumber) {
          // Increment the page number to fetch the next page
          params.pageNumber += 1;
        } else {
          // No more pages available, exit the loop
          hasNextPage = false;
        }
      }

      return [allConnections, keyPairs];
    } catch (e) {
      if (e instanceof AxiosError) {
        throw new Error(JSON.stringify(e));
      } else {
        throw e;
      }
    }
  }

  async importBundles(dsnpUserId: MessageSourceId, graphKeyPairs: ProviderKeyPair[]): Promise<boolean> {
    const importBundles = await this.formImportBundles(dsnpUserId, graphKeyPairs);
    return this.graphStateManager.importUserData(importBundles);
  }

  async formImportBundles(dsnpUserId: MessageSourceId, graphKeyPairs: ProviderKeyPair[]): Promise<ImportBundle[]> {
    const importBundles: ImportBundle[] = [];
    const publicFollowSchemaId = this.graphStateManager.getSchemaIdFromConfig(ConnectionType.Follow, PrivacyType.Public);
    const publicFriendshipSchemaId = this.graphStateManager.getSchemaIdFromConfig(ConnectionType.Friendship, PrivacyType.Public);
    const privateFollowSchemaId = this.graphStateManager.getSchemaIdFromConfig(ConnectionType.Follow, PrivacyType.Private);
    const privateFriendshipSchemaId = this.graphStateManager.getSchemaIdFromConfig(ConnectionType.Friendship, PrivacyType.Private);

    const publicFollows: PaginatedStorageResponse[] = await this.blockchainService.rpc('statefulStorage', 'getPaginatedStorage', dsnpUserId, publicFollowSchemaId);
    const publicFriendships: PaginatedStorageResponse[] = await this.blockchainService.rpc('statefulStorage', 'getPaginatedStorage', dsnpUserId, publicFriendshipSchemaId);
    const privateFollows: PaginatedStorageResponse[] = await this.blockchainService.rpc('statefulStorage', 'getPaginatedStorage', dsnpUserId, privateFollowSchemaId);
    const privateFriendships: PaginatedStorageResponse[] = await this.blockchainService.rpc('statefulStorage', 'getPaginatedStorage', dsnpUserId, privateFriendshipSchemaId);

    const importBundleBuilder = new ImportBundleBuilder();
    // Only X25519 is supported for now
    // check if all keys are of type X25519
    const areKeysCorrectType = graphKeyPairs.every((keyPair) => keyPair.keyType === KeyType.X25519);
    if (!areKeysCorrectType) {
      throw new Error('Only X25519 keys are supported for now');
    }

    importBundles.push(
      ...publicFollows.map((publicFollow) =>
        importBundleBuilder
          .withDsnpUserId(dsnpUserId.toString())
          .withSchemaId(publicFollowSchemaId)
          .withPageData(publicFollow.page_id.toNumber(), publicFollow.payload, publicFollow.content_hash.toNumber())
          .build(),
      ),
    );

    importBundles.push(
      ...publicFriendships.map((publicFriendship) =>
        importBundleBuilder
          .withDsnpUserId(dsnpUserId.toString())
          .withSchemaId(publicFriendshipSchemaId)
          .withPageData(publicFriendship.page_id.toNumber(), publicFriendship.payload, publicFriendship.content_hash.toNumber())
          .build(),
      ),
    );

    if (privateFollows.length > 0 || privateFriendships.length > 0) {
      const dsnpKeys = await this.formDsnpKeys(dsnpUserId);
      const graphKeyPairsSdk = graphKeyPairs.map(
        (keyPair: ProviderKeyPair): GraphKeyPair => ({
          keyType: GraphKeyType.X25519,
          publicKey: keyPair.publicKey,
          secretKey: keyPair.privateKey,
        }),
      );

      importBundles.push(
        ...privateFollows.map((privateFollow) =>
          importBundleBuilder
            .withDsnpUserId(dsnpUserId.toString())
            .withSchemaId(privateFollowSchemaId)
            .withPageData(privateFollow.page_id.toNumber(), privateFollow.payload, privateFollow.content_hash.toNumber())
            .withDsnpKeys(dsnpKeys)
            .withGraphKeyPairs(graphKeyPairsSdk)
            .build(),
        ),
      );

      importBundles.push(
        ...privateFriendships.map((privateFriendship) =>
          importBundleBuilder
            .withDsnpUserId(dsnpUserId.toString())
            .withSchemaId(privateFriendshipSchemaId)
            .withPageData(privateFriendship.page_id.toNumber(), privateFriendship.payload, privateFriendship.content_hash.toNumber())
            .withDsnpKeys(dsnpKeys)
            .withGraphKeyPairs(graphKeyPairsSdk)
            .build(),
        ),
      );
    }
    return importBundles;
  }

  async formConnections(
    dsnpUserId: MessageSourceId | AnyNumber,
    providerId: MessageSourceId | AnyNumber,
    isTransitive: boolean,
    graphConnections: ProviderGraph[],
  ): Promise<ConnectAction[]> {
    const dsnpKeys = await this.formDsnpKeys(dsnpUserId);
    const actions: ConnectAction[] = [];
    
    for (const connection of graphConnections) {
      const connectionType = connection.connectionType.toLowerCase();
      const privacyType = connection.privacyType.toLowerCase();
      const schemaId = this.graphStateManager.getSchemaIdFromConfig(connectionType as ConnectionType, privacyType as PrivacyType);
      /// make sure user has delegation for schemaId
      const isDelegated = await this.blockchainService.rpc('msa', 'grantedSchemaIdsByMsaId', dsnpUserId, providerId);
      /// make sure incoming user connection is also delegated for queuing updates non-transitively
      const isDelegatedConnection = await this.blockchainService.rpc('msa', 'grantedSchemaIdsByMsaId', dsnpUserId, providerId);
      if (
        !isDelegated.isSome ||
        !isDelegated
          .unwrap()
          .map((grant) => grant.schema_id.toNumber())
          .includes(schemaId)
      ) {
        continue;
      }

      switch (connection.direction) {
        case 'connectionTo': {
          const connect: Connection = {
            dsnpUserId: connection.dsnpId,
            schemaId,
          };

          const connectionAction: ConnectAction = {
            type: 'Connect',
            ownerDsnpUserId: dsnpUserId.toString(),
            connection: connect,
          };

          if (dsnpKeys && dsnpKeys.keys.length > 0) {
            connectionAction.dsnpKeys = dsnpKeys;
          }

          actions.push(connectionAction);
          break;
        }
        case 'connectionFrom': {
          if (
            isTransitive &&
            isDelegatedConnection.isSome &&
            isDelegatedConnection
              .unwrap()
              .map((grant) => grant.schema_id.toNumber())
              .includes(schemaId)
          ) {
            const { key: jobId, data } = createGraphUpdateJob(connection.dsnpId, providerId, SkipTransitiveGraphs);
            this.graphUpdateQueue.add('graphUpdate', data, { jobId });
          }
          break;
        }
        case 'bidirectional': {
          const connect: Connection = {
            dsnpUserId: connection.dsnpId,
            schemaId,
          };

          const connectionAction: ConnectAction = {
            type: 'Connect',
            ownerDsnpUserId: dsnpUserId.toString(),
            connection: connect,
          };

          if (dsnpKeys && dsnpKeys.keys.length > 0) {
            connectionAction.dsnpKeys = dsnpKeys;
          }

          actions.push(connectionAction);

          if (
            isTransitive &&
            isDelegatedConnection.isSome &&
            isDelegatedConnection
              .unwrap()
              .map((grant) => grant.schema_id.toNumber())
              .includes(schemaId)
          ) {
            const { key: jobId, data } = createGraphUpdateJob(connection.dsnpId, providerId, SkipTransitiveGraphs);
            this.graphUpdateQueue.add('graphUpdate', data, { jobId });
          }
          break;
        }
        default:
          throw new Error(`Unrecognized connection direction: ${connection.direction}`);
      }
    }

    return actions;
  }

  async formDsnpKeys(dsnpUserId: MessageSourceId | AnyNumber): Promise<DsnpKeys> {
    const publicKeySchemaId = this.graphStateManager.getGraphKeySchemaId();
    const publicKeys: ItemizedStoragePageResponse = await this.blockchainService.rpc('statefulStorage', 'getItemizedStorage', dsnpUserId, publicKeySchemaId);
    const dsnpKeys = {
      dsnpUserId: dsnpUserId.toString(),
      keysHash: publicKeys.content_hash.toNumber(),
      keys: publicKeys.items.map(
        (item: ItemizedStorageResponse): KeyData => ({
          index: item.index.toNumber(),
          content: item.payload.toU8a(),
        }),
      ),
    };
    return dsnpKeys;
  }

  async sendAndProcessChainEvents(
    dsnpUserId: MessageSourceId,
    providerId: ProviderId,
    providerKeys: KeyringPair,
    graphKeyPairs: ProviderKeyPair[],
    batchesMap: SubmittableExtrinsic<'rxjs', ISubmittableResult>[][],
    ): Promise<void> {
    try {
      // iterate over batches and send them to the chain
      batchesMap.forEach(async (batch, batchIndex) => {
        await this.processSingleBatch(dsnpUserId, providerId, providerKeys, graphKeyPairs, batch);
      });
    } catch (e) {
      this.logger.error(`Error processing batches for ${dsnpUserId.toString()}: ${e}`);
      throw e;
    }
  }

  async processSingleBatch(
    dsnpUserId: MessageSourceId,
    provideId: ProviderId,
    providerKeys: KeyringPair,
    graphKeyPairs: ProviderKeyPair[],
    batch: SubmittableExtrinsic<'rxjs', ISubmittableResult>[]
    ): Promise<void> {
    try {
      const [event, eventMap] = await this.blockchainService.createExtrinsic(
        { pallet: 'frequencyTxPayment', extrinsic: 'payWithCapacityBatchAll' }, 
        { eventPallet: 'utility', event: 'BatchCompleted' },
        providerKeys, batch).signAndSend();
      
      if (!event) {
        // if we dont get any events, covering any unexpected connection errors
        throw new Error(`No events were found for ${dsnpUserId.toString()}`);
      }

      if(!this.blockchainService.api.events.utility.BatchCompleted.is(event)) {
        this.logger.warn(`Batch failed event found for ${dsnpUserId.toString()}: ${event}`);
        if(this.blockchainService.api.events.utility.BatchInterrupted.is(event)) {
          // this event should not occur given we are filtering target event in extrinsic
          // call to be `BatchCompleted`
          this.logger.warn(`Unexpected event found for ${dsnpUserId.toString()}`);
          this.logger.warn(event);
          this.logger.warn(eventMap);
          throw new Error(`Batch interrupted event found for ${dsnpUserId.toString()}`);
        }
      }
    } catch (e) {
      this.logger.error(`Error processing batch for ${dsnpUserId.toString()}: ${e}`);
      //Following errors includes are checked against
      // 1. Inability to pay some fees`
      // 2. Transaction is not valid due to `Target page hash does not match current page hash`

      if (e instanceof Error && e.message.includes('Inability to pay some fees')) {
        // in case capacity is low pause the queue
        this.graphUpdateQueue.pause();
        throw e;
      } else if (e instanceof Error && e.message.includes('Target page hash does not match current page hash')) {
        // refresh state and queue a non-transitive graph update
        // this is safe to do as we are only updating single user's graph
        const { key: jobId, data } = createGraphUpdateJob(dsnpUserId, provideId, SkipTransitiveGraphs);
        this.graphUpdateQueue.add('graphUpdate', data, { jobId });
      }
    }
  }
}
