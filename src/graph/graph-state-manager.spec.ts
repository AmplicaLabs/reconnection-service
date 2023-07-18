// ignore moving this to down to the bottom of the file, it will cause the test to fail
// ignore linting this file, it will cause the test to fail
// tslint:disable-next-line: no-var-requires
import { Test, TestingModule } from '@nestjs/testing';
import { Action, ConnectAction, Connection, ConnectionType, DsnpKeys, GraphKeyPair, GraphKeyType, ImportBundle, KeyData, PageData, PrivacyType } from '@dsnp/graph-sdk';
import { ConfigModule } from '@nestjs/config';
import { GraphStateManager } from './graph-state-manager';
import { ConfigService } from '../config/config.service';
import { GraphManagerModule } from './graph-state.module';

describe('GraphStateManager', () => {
  let graphStateManager: GraphStateManager;

  beforeAll(async () => {
    const module: TestingModule = await Test.createTestingModule({
      imports: [
        GraphManagerModule,
        ConfigModule.forRoot({
          isGlobal: true,
        }),
      ],
      providers: [GraphStateManager, ConfigService],
    }).compile();

    graphStateManager = module.get<GraphStateManager>(GraphStateManager);
    await graphStateManager.onApplicationBootstrap();
  });

  it('should be defined', () => {
    expect(graphStateManager).toBeDefined();
  });

  it('should return graph config', async () => {
    const graphConfig = await graphStateManager.getGraphConfig();
    expect(graphConfig).toBeDefined();
  });

  it('should initialize state and import bundle upon request', async () => {
    // Set up import data
    const dsnpUserId1 = 1;
    const dsnpUserId2 = 2;

    const pageData1: PageData = {
      pageId: 1,
      content: new Uint8Array([24, 227, 96, 97, 96, 99, 224, 96, 224, 98, 96, 0, 0]),
      contentHash: 100,
    };

    const keyPairs1: GraphKeyPair[] = [];
    const keyPairs2: GraphKeyPair[] = [];

    const dsnpKeys1: DsnpKeys = {
      dsnpUserId: dsnpUserId1.toString(),
      keysHash: 100,
      keys: [],
    };

    const dsnpKeys2: DsnpKeys = {
      dsnpUserId: dsnpUserId2.toString(),
      keysHash: 100,
      keys: [],
    };

    const importBundle1: ImportBundle = {
      dsnpUserId: dsnpUserId1.toString(),
      schemaId: 1,
      keyPairs: keyPairs1,
      dsnpKeys: dsnpKeys1,
      pages: [pageData1],
    };

    const import_result1 = await graphStateManager.importUserData([importBundle1]);
    expect(import_result1).toBe(true);

    // if import is successful and not state is created, it should have a state
    const graphConfig = await graphStateManager.getGraphConfig();

    expect(graphConfig).toBeDefined();
    expect(graphConfig.maxGraphPageSizeBytes).toBeDefined();

    const exportUpdates = await graphStateManager.exportGraphUpdates();
    expect(exportUpdates).toBeDefined();
    expect(exportUpdates.length).toBe(0);
  });

  it('should apply actions and export graph updates', async () => {
    // Set up actions
    const actions = [] as Action[];
    const action_1 = {
      type: 'Connect',
      ownerDsnpUserId: '10',
      connection: {
        dsnpUserId: '4',
        schemaId: 1,
      } as Connection,
      dsnpKeys: {
        dsnpUserId: '4',
        keysHash: 100,
        keys: [],
      } as DsnpKeys,
    } as ConnectAction;

    actions.push(action_1);

    const applyActionsResult = await graphStateManager.applyActions(actions);
    expect(applyActionsResult).toBe(true);

    const exportUpdates = await graphStateManager.exportGraphUpdates();
    expect(exportUpdates).toBeDefined();
    expect(exportUpdates.length).toBe(1);
  });

  it('getConnectionsWithoutKeys with empty connections should return empty array', async () => {
    const connections = await graphStateManager.getConnectionWithoutKeys();
    expect(connections).toBeDefined();
    expect(connections.length).toBe(0);
  });

  it('getPublicKeys with empty connections should return empty array', async () => {
    const publicKeys = await graphStateManager.getPublicKeys('1');
    expect(publicKeys).toBeDefined();
    expect(publicKeys.length).toBe(0);
  });

  it('Read and deserialize published graph keys', async () => {
    const dsnp_key_owner = 1000;

    // published keys blobs fetched from blockchain
    const published_keys_blob = [
      64, 15, 234, 44, 175, 171, 220, 131, 117, 43, 227, 111, 165, 52, 150, 64, 218, 44, 130, 138, 221, 10, 41, 13, 241, 60, 210, 216, 23, 62, 178, 73, 111,
    ];
    const dsnp_keys = {
      dsnpUserId: dsnp_key_owner.toString(),
      keysHash: 100,
      keys: [
        {
          index: 0,
          content: new Uint8Array(published_keys_blob),
        },
      ] as KeyData[],
    } as DsnpKeys;

    const deserialized_keys = await GraphStateManager.deserializeDsnpKeys(dsnp_keys);
    expect(deserialized_keys).toBeDefined();
  });

  it('generateKeyPair should return a key pair', async () => {
    const keyPair = await GraphStateManager.generateKeyPair(GraphKeyType.X25519);
    expect(keyPair).toBeDefined();
    expect(keyPair.publicKey).toBeDefined();
    expect(keyPair.secretKey).toBeDefined();
  });

  it('should remove user graph', async () => {
    const removeUserGraphResult = await graphStateManager.removeUserGraph('1');
    expect(removeUserGraphResult).toBe(true);
  });

  it('should return false if graph does not contain user', async () => {
    const containsUserGraphResult = await graphStateManager.graphContainsUser('1');
    expect(containsUserGraphResult).toBe(false);
  });

  it('should return schema id for connection type and privacy type', async () => {
    const schemaId = await graphStateManager.getSchemaIdFromConfig(ConnectionType.Follow, PrivacyType.Public);
    expect(schemaId).toBeGreaterThan(0);
  });
});
