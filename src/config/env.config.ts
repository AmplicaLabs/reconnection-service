import Joi from 'joi';
import { ConfigModuleOptions } from '@nestjs/config';
import { mnemonicValidate } from '@polkadot/util-crypto';

export const configModuleOptions: ConfigModuleOptions = {
  isGlobal: true,
  validationSchema: Joi.object({
    REDIS_URL: Joi.string().uri().required(),
    FREQUENCY_URL: Joi.string().uri().required(),
    PROVIDER_BASE_URL: Joi.string().uri().required(),
    PROVIDER_USER_GRAPH_ENDPOINT: Joi.string().required(),
    PROVIDER_ACCESS_TOKEN: Joi.string().required(),
    BLOCKCHAIN_SCAN_INTERVAL_MINUTES: Joi.number()
      .min(1)
      .default(3 * 60),
    QUEUE_HIGH_WATER: Joi.number().min(100).default(1000),
    PROVIDER_ACCOUNT_SEED_PHRASE: Joi.string()
      .required()
      .custom((value: string, helpers) => {
        if (!mnemonicValidate(value)) {
          return helpers.error('any.invalid');
        }
        return value;
      }),
    GRAPH_ENVIRONMENT_TYPE: Joi.string().required().valid('Mainnet', 'Rococo', 'Dev'),
    // GRAPH_ENVIRONMENT_DEV_CONFIG is optional, but if it is set, it must be a valid JSON string
    GRAPH_ENVIRONMENT_DEV_CONFIG: Joi.string().when('GRAPH_ENVIRONMENT_TYPE', {
      is: 'Dev',
      then: Joi.string()
        .required()
        .custom((value: string, helpers) => {
          try {
            JSON.parse(value);
          } catch (e) {
            return helpers.error('any.invalid');
          }
          return value;
        }),
    }),
  }),
};
