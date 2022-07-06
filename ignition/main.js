/**
 * @file Defines the functions to validate a Polyglot Engine request, then add it to
 *  the engine's processing queue.
 * @author LibreTexts <info@libretexts.org>
 */
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

/**
 * Verifies that a variable is a string and is non-empty (when whitespace is removed).
 *
 * @param {string} str - The string to check.
 * @returns {boolean} True if variable is a string and is non-emtpy, false if
 * non-empty or non-string.
 */
function isNonEmptyString(str) {
  return typeof (str) === 'string' && str?.trim().length > 0;
}

/**
 * Parses a LibreTexts URL and extracts the subdomain and relative path.
 *
 * @param {string} url - The URL to parse.
 * @returns {string[]} A 2-tuple containing the subdomain and path, or an empty array.
 */
function parseURL(url) {
  if (isNonEmptyString(url) && url.match(/https?:\/\/.*?\.libretexts\.org/)) {
    const pathArr = url.match(/(?<=https?:\/\/.*?\/).*/);
    const path = pathArr ? pathArr[0] : '';
    const subArr = url.match(/(?<=https?:\/\/).*?(?=\.)/);
    const subdomain = subArr ? subArr[0] : '';
    return [subdomain, path];
  }
  return [];
}

/**
 * Generates an HTTP response object to pass to API Gateway.
 *
 * @param {number} status - The HTTP status code to set in the response.
 * @param {string|object} msg - A message to pass ass the response body.
 * @returns {object} The finalized response object.
 */
function generateHTTPResponse(status, msg) {
  return {
    body: JSON.stringify({ status, msg }),
    statusCode: status.toString(),
    headers: {
      'Content-Type': 'application/json',
    },
  };
}

/**
 * Retrieves the Polyglot Engine authorization passphrase from AWS Systems Manager,
 * then verifies the provided authorization header against it.
 *
 * @param {object} headers - Headers passed with the original request.
 * @returns {Promise<boolean>} True if authorized, false otherwise.
 */
async function verifyAuthorization(headers) {
  try {
    console.log('[VERIFY AUTH]');
    const authField = headers.authorization || headers.Authorization;
    if (!authField || typeof (authField) !== 'string') {
      throw (new Error('Authorization header not provided.'));
    }
    const sysClient = new SSMClient();
    const phraseResponse = await sysClient.send(new GetParameterCommand({
      Name: process.env.AWS_SSM_ENGINE_PASSPHRASE_NAME,
      WithDecryption: true,
    }));
    if (phraseResponse.$metadata.httpStatusCode !== 200) {
      throw (new Error('Unknown error encountered using SSM API.'));
    }
    if (!phraseResponse.Parameter) {
      throw (new Error('Parameter not found or invalid.'));
    }
    const passphrase = phraseResponse.Parameter.Value;
    const authHeader = authField.replace('Bearer ', '');
    return passphrase === authHeader;
  } catch (e) {
    console.error('[VERIFY AUTH] Error occured retrieving or verifying authorization:');
    console.error(e);
  }
  return false;
}

/**
 * Validates the Start Translation request parameters.
 *
 * @param {object} queryParams - The original request query string parameters.
 * @returns {object} 3-tuple containing a boolean (true if all valid), and array of validation
 * error messages (if any found), and an object containing the procesed params (if no errors).
 */
function validateEventParams(queryParams) {
  console.log('[VERIFY PARAMS]');
  const validationErrors = [];
  const notifyAddrs = [];
  if (typeof (queryParams) !== 'object' || !queryParams) {
    validationErrors.push('Invalid parameters object provided.');
  }
  if (typeof (queryParams.url) !== 'string' || queryParams.url.trim().length < 1) {
    validationErrors.push('URL not provided or invalid form.');
  }
  if (typeof (queryParams.targetpath) !== 'string' || queryParams.targetpath.trim().length < 1) {
    validationErrors.push('Target path not provided or invalid form.');
  }
  if (typeof (queryParams.language) !== 'string' || queryParams.language.trim().length < 1) {
    validationErrors.push('Language code not provided or invalid.');
  }
  if (typeof (queryParams.notify) === 'string') {
    const inputAddrs = queryParams.notify.split(',');
    inputAddrs.forEach((email) => {
      const trimmed = email.trim();
      if (email.includes('@') && trimmed.length > 0) {
        notifyAddrs.push(trimmed);
      } else {
        validationErrors.push(`Provided address ${email} is invalid.`);
      }
    });
  }
  const [lib, path] = parseURL(queryParams.url);
  const [targetLib, targetPath] = parseURL(queryParams.targetpath);
  if (!lib || !path) {
    validationErrors.push('Invalid URL to translate.');
  }
  if (!targetLib || !targetPath) {
    validationErrors.push('Invalid target URL.');
  }
  const errorsFound = validationErrors.length > 0;
  if (errorsFound) {
    console.error('[PROCESS PARAMS] Parameters validation failed:');
    validationErrors.forEach((err) => console.error(`\t${err}`));
  }
  const foundParams = {
    language: queryParams.language,
    lib,
    path,
    targetLib,
    targetPath,
    notifyAddrs,
  };
  return [!errorsFound, validationErrors, foundParams];
}

/**
 * Runs validation on the translation request parameters, then pushes it to the
 * engine processing queue.
 *
 * @param {object} event - The initial Lambda invocation event.
 * @returns {Promise<object>} Returns an HTTP response object to pass to the
 *  requester with status indicated.
 */
async function engineIgnition(event) {
  console.log('[ENGINE IGNITION] Attempting request ignition...');
  const authorized = await verifyAuthorization(event.headers);
  if (!authorized) {
    console.error('Fatal Error: Unauthorized Request.');
    return generateHTTPResponse(401, 'Polyglot Engine: Invalid authorization passphrase.');
  }

  const [validParams, paramErrs, foundParams] = validateEventParams(event.queryStringParameters);
  if (!validParams) {
    console.error('Fatal Error: Required parameters are missing.');
    return generateHTTPResponse(400, {
      msg: 'Polyglot Engine: Required parameters are missing.',
      errors: paramErrs,
    });
  }

  console.log('[ENGINE IGNITION] Sending queue message...');
  const queueMsg = { originalParams: event.queryStringParameters, ...foundParams };
  const sqsClient = new SQSClient();
  const queueRes = await sqsClient.send(new SendMessageCommand({
    MessageBody: JSON.stringify(queueMsg),
    MessageGroupId: process.env.AWS_SQS_GROUP_ID,
    QueueUrl: process.env.AWS_SQS_QUEUE_URL,
  }));
  if (queueRes?.$metadata?.httpStatusCode !== 200) {
    return generateHTTPResponse(500, 'Polyglot Engine: Unknown internal error occurred.');
  }
  console.log('[ENGINE IGNITION] Request queued succesfully!');
  return generateHTTPResponse(200, 'Polyglot Engine: Translation request successfully queued.');
}

/**
 * Exposes the main function to the Lambda execution environment.
 *
 * @param {object} event - The event that triggered the Lambda invocation.
 * @returns {Promise<boolean>} True if process succeeded, false otherwise.
 */
export default async function handleEngineIgnition(event) {
  console.log(event);
  if (!event) {
    console.error('[HANDLER] Event information is invalid or missing.');
    return false;
  }
  return engineIgnition(event);
}
