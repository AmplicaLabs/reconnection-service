import '@frequency-chain/api-augment';
import { MessageSourceId } from '@frequency-chain/api-augment/interfaces';
import { Keyring } from '@polkadot/keyring';
import { KeyringPair } from '@polkadot/keyring/types';
import { u32 } from '@polkadot/types';
import { cryptoWaitReady } from '@polkadot/util-crypto';
import { AddProviderPayload, ExtrinsicHelper, UserBuilder, signPayloadSr25519, initialize } from 'frequency-scenario-template'
import log from 'loglevel';
import { ProviderGraph, GraphKeyPair } from "reconnection-service/src/interfaces/provider-graph.interface"
import fs from 'node:fs';

type ChainUser = {
    uri?: string;
    keys?: KeyringPair;
    msaId?: MessageSourceId;
}

type ProviderResponse = {
    dsnpId: string;
    connections: {
        data: ProviderGraph[],
        pagination?: {
            pageNumber: number;
            pageSize: number;
            pageCount: number;
        },
    },
    graphKeyPairs: GraphKeyPair[]
}

const keyring = new Keyring({ type: 'sr25519' });
const DEFAULT_SCHEMAS = [5, 7, 8, 9, 10];
let nonce: u32;

async function createUserWithProvider(follower: ChainUser, provider: ChainUser) {
    const existingUser = await ExtrinsicHelper.apiPromise.query.msa.publicKeyToMsaId(follower.keys!.publicKey);
    if (existingUser.isSome) {
        follower.msaId = existingUser.unwrap();
        console.log(`Existing user ${follower.msaId.toString()} found`);
        return;
    }
    const block = await ExtrinsicHelper.apiPromise.rpc.chain.getBlock();
    const blockNumber = block.block.header.number.toNumber();
    const addProvider: AddProviderPayload = {
        authorizedMsaId: provider.msaId,
        schemaIds: DEFAULT_SCHEMAS,
        expiration: blockNumber + 50,
    }
    const payload = ExtrinsicHelper.apiPromise.registry.createType('PalletMsaAddProvider', addProvider);
    const proof = signPayloadSr25519(follower.keys!, payload);
    const [creationEvent] = await ExtrinsicHelper.createSponsoredAccountWithDelegation(follower.keys!, provider.keys!, proof, addProvider).payWithCapacity();
    // eslint-disable-next-line new-cap
    // nonce = new u32(ExtrinsicHelper.apiPromise.registry, nonce.iaddn(1));
    if (creationEvent && ExtrinsicHelper.apiPromise.events.msa.MsaCreated.is(creationEvent)) {
        follower.msaId = creationEvent.data.msaId;
        console.log(`Created follower ${creationEvent.data.msaId.toString()}`);
    }
}

async function main() {
    await cryptoWaitReady();
    console.log('Connecting...');
    await initialize('ws://127.0.0.1:9944');
    log.setLevel('trace');
    const { apiPromise } = ExtrinsicHelper;

    // Create all keypairs
    const provider: ChainUser = { keys: keyring.addFromUri('//Alice') };
    const famousUser: ChainUser = { uri: '//Bob', keys: keyring.addFromUri('//Bob') };
    const followers: ChainUser[] = new Array(7_000).fill(0).map((_, index) => {
        console.log(`Creating keypair for //Charlie//${index}`);
        return { uri: `//Charlie//${index}`, keys: keyring.addFromUri(`//Charlie//${index}`)};
     });
    console.log('Created keypairs');

    // Create graph keys for all users

    // Create provider
    console.log('Creating provider...');
    const builder = new UserBuilder();
    const providerUser = await builder.withKeypair(provider.keys!).asProvider('Alice Provider').withFundingSource(provider.keys).build();
    provider.msaId = providerUser.msaId;
    await ExtrinsicHelper.stake(provider.keys!, provider.msaId, 100_000_000_000).signAndSend();
    console.log(`Created provider ${provider.msaId.toString()}`);

    nonce = (await apiPromise.query.system.account(provider.keys!.publicKey)).nonce;

    // Create followers
    // eslint-disable-next-line no-restricted-syntax
    for (const follower of followers) {
        // eslint-disable-next-line no-await-in-loop
        await createUserWithProvider(follower, provider);
    }

    // Create famous, followed user
    // NOTE: Must be AFTER other users created, we want this to be the last event on the chain
    await createUserWithProvider(famousUser, provider);
    const famousBlock = await apiPromise.rpc.chain.getBlock();
    console.log('Famous user created at block: ', famousBlock.block.header.number.toNumber());

    // Create JSON responses for each follower
    if (!fs.existsSync('../../webhook-specification/mock-webhook-server/responses')) {
        fs.mkdirSync('../../webhook-specification/mock-webhook-server/responses');
    };
    followers.forEach((follower) => {
        const response: ProviderResponse = {
            dsnpId: follower.msaId!.toString(),
            connections: {
                data: [
                    {
                        dsnpId: famousUser.msaId!.toString(),
                        privacyType: 'Private',
                        direction: 'connectionTo',
                        connectionType: 'Follow'
                    }
                ]
            },
            graphKeyPairs: []
        }

        fs.writeFileSync(`../../webhook-specification/mock-webhook-server/responses/response.${follower.msaId?.toString()}.json`, JSON.stringify(response, undefined, 4));
    });

    const famousUserResponse = {
        dsnpId: famousUser.msaId!.toString(),
        graphKeyPairs: [
            {
            keyType: "X25519",
            publicKey: "0xe3b18e1aa5c84175ec0c516838fb89dd9c947dd348fa38fe2082764bbc82a86f",
            privateKey: "0xa55688dc2ffcf0f2b7e819029ad686a3c896c585725f5ac38dddace7de703451"
            } as GraphKeyPair
        ]
    }

    const responses: ProviderResponse[] = [];
    while (followers.length > 0) {
        const followerSlice = followers.splice(0, Math.min(followers.length, 100));
        const response = {
            ...famousUserResponse,
            connections: {
                data: followerSlice.map((follower) => ({
                    dsnpId: follower.msaId!.toString(),
                    privacyType: 'Private',
                    direction: "connectionFrom",
                    connectionType: 'Follow'
                } as ProviderGraph))
            }
        }
        responses.push(response);
    }

    responses.forEach((response, index) => {
        response.connections.pagination = {
            pageNumber: index + 1,
            pageCount: responses.length,
            pageSize: response.connections.data.length
        }
        fs.writeFileSync(`../../webhook-specification/mock-webhook-server/responses/response.${famousUser.msaId!.toString()}.${index + 1}.json`, JSON.stringify(response, undefined, 4));
    })
}

main().catch(e => console.error(e)).finally(async () => ExtrinsicHelper.disconnect());
