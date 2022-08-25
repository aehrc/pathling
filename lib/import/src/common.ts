/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

/**
 * @author John Grimes
 */

import axios, { AxiosInstance, AxiosResponse } from "axios";
import ClientOAuth2 from "client-oauth2";

export const JSON_CONTENT_TYPE = "application/json";
export const FHIR_JSON_CONTENT_TYPE = "application/fhir+json";
export const FHIR_NDJSON_CONTENT_TYPE = "application/fhir+ndjson";

type SmartConfiguration = { token_endpoint?: string };

export type Validator<UnvalidatedType, ResponseType> = (
  response: AxiosResponse<UnvalidatedType>
) => AxiosResponse<ResponseType>;

export type MaybeAuthenticated = WithAuthentication | WithoutAuthentication;

export interface MaybeValidated<UnvalidatedType, ResponseType> {
  validator?: Validator<UnvalidatedType, ResponseType>;
}

export interface WithAuthentication {
  authenticationEnabled: true;
  clientId: string;
  clientSecret: string;
  scopes?: string;
}

export interface WithoutAuthentication {
  authenticationEnabled: false;
}

export async function getToken(
  tokenUrl: string,
  clientId: string,
  clientSecret: string,
  scopes?: string
): Promise<ClientOAuth2.Token> {
  const exportAuth = new ClientOAuth2({
    clientId: clientId,
    clientSecret: clientSecret,
    accessTokenUri: tokenUrl,
    scopes: scopes ? scopes.split(" ") : [],
  });
  const token = await exportAuth.credentials.getToken();
  // noinspection JSUnresolvedVariable
  console.info("Token successfully retrieved for client: %s", clientId);
  return token;
}

export async function buildClient<
  UnvalidatedType = unknown,
  ResponseType = unknown
>(
  options: { endpoint: string } & MaybeAuthenticated &
    MaybeValidated<UnvalidatedType, ResponseType>
) {
  const { endpoint, validator, authenticationEnabled } = options;
  if (authenticationEnabled) {
    const { clientId, clientSecret, scopes } = options;
    return buildAuthenticatedClient(
      endpoint,
      clientId,
      clientSecret,
      scopes,
      validator
    );
  } else {
    return Promise.resolve(buildUnauthenticatedClient(endpoint, validator));
  }
}

export function buildUnauthenticatedClient<UnvalidatedType, ResponseType>(
  endpoint: string,
  validator?: Validator<UnvalidatedType, ResponseType>
): AxiosInstance {
  const instance = axios.create();
  instance.defaults.baseURL = endpoint;
  if (validator) {
    instance.interceptors.response.use(validator);
  }
  return instance;
}

export async function buildAuthenticatedClient<UnvalidatedType, ResponseType>(
  endpoint: string,
  clientId: string,
  clientSecret: string,
  scopes?: string,
  validator?: Validator<UnvalidatedType, ResponseType>
): Promise<AxiosInstance> {
  const tokenUrl = await autodiscoverTokenUrl(endpoint);
  if (!tokenUrl) {
    throw new Error("Token URL not found within SMART configuration document");
  }
  const token = await getToken(tokenUrl, clientId, clientSecret, scopes);
  const instance = buildUnauthenticatedClient(endpoint, validator);
  instance.defaults.headers.common[
    "Authorization"
  ] = `Bearer ${token.accessToken}`;
  return instance;
}

export async function autodiscoverTokenUrl(
  endpoint: string
): Promise<string | undefined> {
  const response = await axios.get<
    undefined,
    AxiosResponse<SmartConfiguration>
  >(`${endpoint}/.well-known/smart-configuration`, {
    headers: {
      Accept: JSON_CONTENT_TYPE,
    },
  });
  return response.data["token_endpoint"];
}

export function getStatusUrl<T>(response: AxiosResponse<T>) {
  const statusUrl = response.headers["content-location"];
  if (!statusUrl) {
    throw new Error("No Content-Location header found");
  }
  return statusUrl;
}

export function validateAsyncResponse(response: AxiosResponse<unknown>) {
  if (response.status !== 202) {
    throw new Error("Response from async request was not 202 Accepted");
  }
  return response;
}
