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

export async function getToken(
  tokenUrl: string,
  clientId: string,
  clientSecret: string,
  scopes: string
): Promise<ClientOAuth2.Token> {
  const exportAuth = new ClientOAuth2({
    clientId: clientId,
    clientSecret: clientSecret,
    accessTokenUri: tokenUrl,
    scopes: scopes ? scopes.split(" ") : []
  });
  const token = await exportAuth.credentials.getToken();
  // noinspection JSUnresolvedVariable
  console.info("Token successfully retrieved for client: %s", clientId);
  return token;
}

export async function buildAuthenticatedClient(
  endpoint: string,
  clientId: string,
  clientSecret: string,
  scopes: string
): Promise<AxiosInstance> {
  const tokenUrl = await autodiscoverTokenUrl(endpoint);
  if (!tokenUrl) {
    throw "Token URL not found within SMART configuration document";
  }
  const token = await getToken(tokenUrl, clientId, clientSecret, scopes);
  const instance = axios.create();
  instance.defaults.baseURL = endpoint;
  instance.defaults.headers.common[
    "Authorization"
    ] = `Bearer ${token.accessToken}`;
  return instance;
}

export async function autodiscoverTokenUrl(
  endpoint: string
): Promise<string | undefined> {
  const response = await axios.get<undefined,
    AxiosResponse<SmartConfiguration>>(`${endpoint}/.well-known/smart-configuration`, {
    headers: {
      Accept: JSON_CONTENT_TYPE
    }
  });
  return response.data["token_endpoint"];
}

export function getStatusUrl<T>(response: AxiosResponse<T>) {
  const statusUrl = response.headers["content-location"];
  if (!statusUrl) {
    throw "No Content-Location header found";
  }
  return statusUrl;
}
