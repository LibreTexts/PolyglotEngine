/**
 * @file Defines functions to process a translated LibreText and save it to a LibreTexts library.
 * @author LibreTexts <info@libretexts.org>
 */
import { Buffer } from 'buffer';
import crypto from 'crypto';
import * as https from 'https';
import { Readable } from 'stream';
import async from 'async';
import axios from 'axios';
import bluebird from 'bluebird';
import * as cheerio from 'cheerio';
import xmlEscape from 'xml-escape';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SESv2Client, SendEmailCommand } from '@aws-sdk/client-sesv2';
import { SSMClient, GetParametersByPathCommand } from '@aws-sdk/client-ssm';
import { TranslateClient, DescribeTextTranslationJobCommand } from '@aws-sdk/client-translate';

const Promise = bluebird;
const LIBREBOT = 'LibreBot';
const ONE_SECOND = 1000;
const MAX_CONCURRENT = 2;

let axiosInstance;
let sourceLibName;
let sourceLibKey;
let sourceLibSecret;
let targetLibName;
let targetLibKey;
let targetLibSecret;

/**
 * Object containing information about a CXone Expert page's special properties.
 *
 * @typedef {object} PageProp
 * @property {string} name - The internal name of the property.
 * @property {string} value - The text value of the property.
 */

/**
 * An internal representation of a LibreText page and its metadata, rebuilt during
 * program execution.
 *
 * @typedef {object} LibreTranslatedPage
 * @property {string} lib - The source internal LibreTexts library shortname/identifier.
 * @property {number} id - The source page identifier number.
 * @property {string} title - The page's UI title.
 * @property {string} contents - The HTML contents of the page.
 * @property {string[]} [tags] - An array of a page's tags and their values.
 * @property {LibreTranslatedPage[]} [subpages] - An array of "subpages" underneath the page
 *  in the hierarchy (can be deeply nested).
 * @property {boolean} [root] - Indicates if the page is the starting page in a text's hierarchy.
 * @property {string} [parent] - Identifies the page's parent in the text hierarchy.
 * @property {string} [url] - The source page's full URL.
 * @property {string} [path] - The source page's path, relative to the library hostname.
 * @property {string} [urlNumPrefix] - The source page's section number prefix (if found).
 * @property {string} [urlTitleExtract] - The source page's extract section title (if found).
 * @property {string} [summary] - The page's overview/summary text.
 * @property {PageProp[]} [props] - The page's special properties.
 */

/**
 * Configures a then-able program execution pause using setTimeout.
 *
 * @param {number} ms - The length of time (in milliseconds) to pause program execution.
 * @returns {Promise<Function>} The setTimeout for the number of specified milliseconds.
 */
const snooze = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
 * Accepts a Readable and returns its contents as a string.
 *
 * @param {Readable} stream - The stream to parse.
 * @returns {Promise<string>} The stream's contents.
 */
async function readableToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (data) => chunks.push(Buffer.from(data)));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
  });
}

/**
 * Assembles a url given an array of parts.
 *
 * @param {string[]} parts - Array of strings to include in the URL, in desired order.
 * @returns {string} The assembled url, or empty string if error encountered.
 */
function assembleUrl(parts) {
  if (!Array.isArray(parts)) {
    console.error('[ASSEMBLE URL] Invalid array of parts provided.');
    return '';
  }
  let url = '';
  for (let i = 0, n = parts.length; i < n; i += 1) {
    const currPart = parts[i];
    if (isNonEmptyString(currPart)) {
      if (!url.endsWith('/') && url.trim().length > 1) {
        url = `${url}/`;
      }
      url = `${url}${currPart}`;
    }
  }
  return url;
}

/**
 * Creates an XML string with a listing of CXone Expert page tags.
 *
 * @param {string[]} tags - An array of tags in 'name:value' format.
 * @returns {string} The XML string containing the tags element.
 */
function createTagsXML(tags) {
  if (!Array.isArray(tags)) {
    throw (new Error('Invalid tags provided.'));
  }
  const tagValues = tags.map((tag) => `<tag value="${xmlEscape(tag)}" />`).join('');
  return `<?xml version="1.0" encoding="UTF-8"?><tags>${tagValues}</tags>`;
}

/**
 * Generates HTTP request headers for use with the CXone (LibreText libraries) API.
 *
 * @param {string} lib - The internal shortname/identifier of the library to access.
 * @returns {object} An object containing the headers to add to the request.
 */
function generateAPIRequestHeaders(lib) {
  let keyToUse;
  let secretToUse;
  if (lib === sourceLibName) {
    keyToUse = sourceLibKey;
    secretToUse = sourceLibSecret;
  } else {
    keyToUse = targetLibKey;
    secretToUse = targetLibSecret;
  }
  const epoch = Math.floor(Date.now() / ONE_SECOND);
  const hmac = crypto.createHmac('sha256', secretToUse);
  hmac.update(`${keyToUse}${epoch}=${LIBREBOT}`);
  return {
    'X-Requested-With': 'XMLHttpRequest',
    'X-Deki-Token': `${keyToUse}_${epoch}_=${LIBREBOT}_${hmac.digest('hex')}`,
  };
}

/**
 * Retrieves an S3 file's contents.
 *
 * @param {S3Client} s3Client - An instantiated S3Client object.
 * @param {string} bucket - The name of the bucket to retrieve file from.
 * @param {string} key - The file's key/path.
 * @returns {Promise<string|null>} The file's contents, or null if error encountered.
 */
async function getFileContents(s3Client, bucket, key) {
  if (!s3Client || !bucket || !key) {
    console.error('[GET CONTENTS] Invalid or missing parameters.');
    return null;
  }
  const formattedKey = key.replace('s3://', '').replace(`${bucket}/`, '');
  console.log(`[GET CONTENTS] ${formattedKey}`);
  const fileResponse = await s3Client.send(new GetObjectCommand({
    Bucket: bucket,
    Key: formattedKey,
  }));
  if (fileResponse.$metadata?.httpStatusCode !== 200) {
    console.error('[GET CONTENTS] Error retrieving file from S3.');
    return null;
  }
  try {
    const fileContents = await readableToString(fileResponse.Body);
    return fileContents;
  } catch (e) {
    console.error('[GET CONTENTS] Error parsing file contents:');
    console.error(e);
  }
  return null;
}

/**
 * Retrieves the CXone Expert key-secret pair for a given library and initializes the
 * global variables for use with the Expert API.
 *
 * @param {boolean} [src = false] - If the keys being retrieved are for the source library.
 * @returns {Promise<boolean>} True if successful, false if error encountered.
 */
async function retrieveLibraryParameters(src = false) {
  try {
    let lib;
    if (src) {
      lib = sourceLibName;
    } else {
      lib = targetLibName;
    }
    console.log(`[RETRIEVE PARAMS] ${lib}`);
    const ssmClient = new SSMClient();
    const keysResponse = await ssmClient.send(new GetParametersByPathCommand({
      Path: `${process.env.AWS_SSM_LIB_SERVERKEYS_PATH}${lib}`,
      MaxResults: 10,
      Recursive: true,
      WithDecryption: true,
    }));
    if (keysResponse.$metadata.httpStatusCode !== 200) {
      throw (new Error('Unknown error encountered using SSM API.'));
    }
    if (!Array.isArray(keysResponse.Parameters)) {
      throw (new Error('Invalid parameters received.'));
    }
    const { Parameters } = keysResponse;
    const libraryKey = Parameters.find((param) => param.Name?.includes(`${lib}/key`));
    const librarySec = Parameters.find((param) => param.Name?.includes(`${lib}/secret`));
    if (libraryKey === undefined || librarySec === undefined) {
      throw (new Error(`Error retrieving key or secret for "${lib}".`));
    }
    if (src) {
      sourceLibKey = libraryKey.Value;
      sourceLibSecret = librarySec.Value;
    } else {
      targetLibKey = libraryKey.Value;
      targetLibSecret = librarySec.Value;
    }
    return true;
  } catch (e) {
    console.error('[RETRIEVE PARAMS] Error retrieving library keys:');
    console.error(e);
  }
  return false;
}

/**
 * Queries the Translate API for the S3 URI of the translation job's output details file.
 *
 * @param {string} jobID - The internal Translate job identifier.
 * @returns {Promise<string|null>} The output file S3 URI, or null if error encountered.
 */
async function getTranslationDetailsURI(jobID) {
  if (!isNonEmptyString(jobID)) {
    console.error(`[RETRIEVE JOB FILE] Invalid jobID provided: "${jobID}".`);
  }
  console.log('[RETRIEVE JOB FILE] Retrieving translation job output URI...');
  try {
    const transClient = new TranslateClient({
      credentials: {
        accessKeyId: process.env.AWS_TRANS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_TRANS_SECRET_ACCESS_KEY,
      },
      region: process.env.AWS_ENGINE_REGION,
    });
    const details = await transClient.send(new DescribeTextTranslationJobCommand({
      JobId: jobID,
    }));
    if (details.$metadata.httpStatusCode !== 200) {
      throw (new Error('Unknown error encountered using Translate API.'));
    }
    const outputFolder = details.TextTranslationJobProperties.OutputDataConfig.S3Uri;
    const outputLangCode = details.TextTranslationJobProperties.TargetLanguageCodes;
    return `${outputFolder}details/${outputLangCode}.auxiliary-translation-details.json`;
  } catch (e) {
    console.error('[RETRIEVE JOB FILE] Error retrieving job output URI:');
    console.error(e);
    return null;
  }
}

/**
 * Retrieves and processes the AWS Translate-generated job metadata file.
 *
 * @param {S3Client} s3Client - An instantiated S3Client object.
 * @param {string} filename - The filename/key/path of the job metadata file.
 * @returns {Promise<object|null>} An object containing the parsed metadata and the
 *  input text's library and pageID.
 */
async function retrieveTranslationJobDetails(s3Client, filename) {
  if (s3Client === null) {
    console.error('[RETRIEVE JOB DETAILS] No S3 Client provided.');
    return null;
  }
  if (!isNonEmptyString(filename)) {
    console.error(`[RETRIEVE JOB DETAILS] Invalid filename provided: "${filename}".`);
    return null;
  }
  console.log('[RETRIEVE JOB DETAILS] Retrieving translation job metadata...');
  const detailsFileContents = await getFileContents(
    s3Client,
    process.env.AWS_S3_OUTPUT_BUCKET,
    filename,
  );
  if (detailsFileContents === null) {
    console.error('[RETRIEVE JOB DETAILS] Error retreiving translation job metadata.');
    return null;
  }
  let detailsFile;
  try {
    detailsFile = JSON.parse(detailsFileContents);
  } catch (e) {
    console.error('[RETRIEVE JOB DETAILS] Error parsing translation job metadata:');
    console.error(e);
    return null;
  }
  if (!Array.isArray(detailsFile.details)) {
    console.error('[RETREIVE JOB DETAILS] Error reading list of files from translation job metadata.');
    return null;
  }
  const {
    sourceLanguageCode,
    targetLanguageCode,
    charactersTranslated,
    inputDataPrefix,
    outputDataPrefix,
    details,
  } = detailsFile;
  const inputPath = inputDataPrefix.replace('s3://', '');
  const [, inputCoverID] = inputPath.split('/');
  if (!isNonEmptyString(inputCoverID)) {
    console.error(`[RETRIEVE JOB DETAILS] Error parsing input coverID from path "${inputPath}".`);
    return null;
  }
  const [lib, id] = inputCoverID.split('-');
  if (!isNonEmptyString(lib) || !isNonEmptyString(id)) {
    console.error(`[RETRIEVE JOB DETAILS] Error parsing coverID "${inputCoverID}".`);
    return null;
  }
  const clientErrors = Number.parseInt(detailsFile.documentCountWithCustomerError, 10);
  const serverErrors = Number.parseInt(detailsFile.documentCountWithServerError, 10);
  if (Number.isNaN(clientErrors) || Number.isNaN(serverErrors)) {
    console.error('[RETRIEVE JOB DETAILS] Error parsing error counts.');
    return null;
  }
  return {
    lib,
    id,
    sourceLanguageCode,
    targetLanguageCode,
    charactersTranslated,
    inputDataPrefix,
    outputDataPrefix,
    details,
    clientErrors,
    serverErrors,
  };
}

/**
 * Retrieves metadata from S3 about the original Engine request and parses it to an object.
 *
 * @param {S3Client} s3Client - An instantiated S3Client object.
 * @param {string} filename - The filename/key/path of the metadata file.
 * @returns {Promise<object>} Object containing the original Engine request metadata.
 */
async function retrieveInputMetadata(s3Client, filename) {
  if (s3Client === null) {
    console.error('[RETRIEVE INPUT DETAILS] No S3 Client provided.');
    return null;
  }
  if (!isNonEmptyString(filename)) {
    console.error(`[RETRIEVE INPUT DETAILS] Invalid filename provided: "${filename}".`);
    return null;
  }
  console.log('[RETRIEVE INPUT DETAILS] Retrieving input metadata...');
  const metaContents = await getFileContents(s3Client, process.env.AWS_S3_OUTPUT_BUCKET, filename);
  if (metaContents === null) {
    return null; // error logged in previous call
  }
  let metadata;
  try {
    metadata = JSON.parse(metaContents);
    const { targetLib, targetPath, allPages } = metadata;
    if (!isNonEmptyString(targetLib)) {
      throw (new Error('Target Library not found or invalid.'));
    }
    if (!isNonEmptyString(targetPath)) {
      throw (new Error('Target Path not found or invalid.'));
    }
    if (!Array.isArray(allPages)) {
      throw (new Error('Input pages not found or invalid.'));
    }
  } catch (e) {
    console.error('[RETRIEVE INPUT DETAILS] Error parsing input metadata:');
    console.error(e);
    return null;
  }
  return metadata;
}

/**
 * Sends a completion message to the email addresses specified in the original
 * translation request, if applicable.
 *
 * @param {string[]} notifyAddrs - Email addresses to send the message to.
 * @param {string} sourceLib - The LibreTexts library shortname of the original content.
 * @param {string} sourceID - The pageID of the original content root.
 * @param {string} targetLib - The LibreTexts library shortname the content was saved to.
 * @param {string} targetPath - The root path the content was saved under.
 * @returns {Promise<boolean>} True if message(s) were sent (or no emails specified),
 *  false otherwise.
 */
async function sendCompletionNotification(notifyAddrs, sourceLib, sourceID, targetLib, targetPath) {
  if (!Array.isArray(notifyAddrs) || notifyAddrs.length < 1) {
    return true;
  }
  try {
    const origTextLink = `https://${sourceLib}.libretexts.org/@go/page/${sourceID}`;
    const trnsTextLink = assembleUrl([`https://${targetLib}.libretexts.org/`, targetPath]);
    const sesClient = new SESv2Client();
    const emailRes = await sesClient.send(new SendEmailCommand({
      Content: {
        Simple: {
          Subject: {
            Data: 'Polyglot Engine: Text Translation Complete',
          },
          Body: {
            Html: {
              Data: `
                <p>The Polyglot Engine has finished processing your request to translate 
                  <a href="${origTextLink}" target="_blank" rel="noopener noreferrer">${sourceLib}-${sourceID}</a>.
                </p>
                <p>The translated text should now be available under: 
                  <a href="${trnsTextLink}" target="_blank" rel="noopener noreferrer">${trnsTextLink}</a>.
                </p>
              `,
            },
          },
        },
      },
      Destination: {
        ToAddresses: notifyAddrs,
      },
      FromEmailAddress: process.env.NOTIFY_FROM_ADDR,
    }));
    if (emailRes.$metadata.httpStatusCode !== 200) {
      console.warn('[SEND NOTIFICATION] Error returned from SES API.');
      return false;
    }
  } catch (e) {
    console.warn('[SEND NOTIFICATION] Error sending a completion notification:');
    console.warn(e);
    return false;
  }
  return true;
}

/**
 * Retrieves translated content from S3 and performs post-translation processing on the content.
 *
 * @param {S3Client} s3Client - An insantiated S3Client object.
 * @param {string} filename - The filename/key/path of the content to retrieve.
 * @returns {Promise<LibreTranslatedPage>} An object containing the parsed/extracted library,
 * original pageID, translated title and contents, or null if error encountered.
 */
async function retrieveAndProcessTranslatedContent(s3Client, filename) {
  if (s3Client === null) {
    console.error('[RETRIEVE+PROCESS] No S3 client provided.');
    return null;
  }
  if (!isNonEmptyString(filename)) {
    console.error(`[RETRIEVE+PROCESS] Invalid filename "${filename}".`);
    return null;
  }
  console.log('[RETRIEVE+PROCESS] Retrieving and processing translated content...');
  const translatedContents = await getFileContents(
    s3Client,
    process.env.AWS_S3_OUTPUT_BUCKET,
    filename,
  );
  if (!isNonEmptyString(translatedContents)) {
    console.error('[RETRIEVE+PROCESS] Invalid contents provided.');
    return null;
  }
  const $ = cheerio.load(translatedContents, { decodeEntities: true }, false);
  const pageTitleSpan = $('span[data-libre-pagetitle="true"]').first();
  if (pageTitleSpan === null) {
    console.error('[RETRIEVE+PROCESS] Error extracting page title from translated content.');
    return null;
  }
  const lib = pageTitleSpan.data('libre-lib');
  const id = pageTitleSpan.data('libre-pageid');
  if (!lib || !id) {
    console.error(`[RETRIEVE+PROCESS] Error parsing metadata element: ${pageTitleSpan.html()}`);
    return null;
  }
  const pageSummaryElem = $('p[data-libre-pagesummary="true"]').first();
  let pageSummary = null;
  if (pageSummaryElem) {
    pageSummary = pageSummaryElem.text();
    pageSummaryElem.remove();
  }
  const translatedTitle = pageTitleSpan.text();
  pageTitleSpan.remove();
  return {
    title: translatedTitle,
    contents: $.html(),
    id: id.toString(),
    summary: pageSummary,
    lib,
  };
}

/**
 * Recursively creates a hierarchical page structure by examining "subpage" or "parent" references.
 *
 * @param {object} page - The page to create a subpage structure for.
 * @param {LibreTranslatedPage[]} inputPages - A flat array of potential subpages.
 * @returns {object} The page with new subpages attached, if found.
 */
function createPageStructure(page, inputPages) {
  console.log(`[CREATE STRUCTURE] ${page.lib}-${page.id}`);
  const pages = inputPages;
  const currPage = page;
  const foundSubpages = [];
  for (let i = 0, n = pages.length; i < n; i += 1) {
    const subpage = pages[i];
    if (!subpage.root && subpage.parent === currPage.id) {
      foundSubpages.push(createPageStructure(subpage, pages)); // recursively create structure
    }
  }
  currPage.subpages = foundSubpages;
  return currPage;
}

/**
 * Merges data from the input metadata and the translated content pages, then recreates the
 * original hierarchical structure.
 *
 * @param {object} inputMetadata - Original Engine input metadata object.
 * @param {LibreTranslatedPage[]} translatedPages - Array of pages after translation.
 * @returns {object} The root page containing the hierarchical structure.
 */
function mergeInputStructure(inputMetadata, translatedPages) {
  if (inputMetadata === null || !Array.isArray(inputMetadata.allPages)) {
    console.error('[MERGE STRUCTURE] Input metadata not provided.');
    return null;
  }
  if (!Array.isArray(translatedPages)) {
    console.error('[MERGE STRUCTURE] Invalid translated listings provided.');
    return null;
  }
  console.log('[MERGE STRUCTURE] Merging input structure...');
  /* Retrieve more information from the input structure and add it to the translated data */
  const pagesData = translatedPages.map((page) => {
    const foundInput = inputMetadata.allPages.find((inputPage) => (
      page.lib === inputPage.lib && page.id === inputPage.id
    ));
    if (foundInput !== undefined) {
      const {
        root,
        parent,
        tags,
        props,
        path,
        urlNumPrefix,
        urlTitleExtract,
      } = foundInput;
      /* don't override translated metadata */
      return {
        ...page,
        root,
        parent,
        tags,
        props,
        path,
        urlNumPrefix,
        urlTitleExtract,
      };
    }
    console.warn(`[MERGE STRUCTURE] WARNING: Matching pages not found for ${page.lib}-${page.id}`);
    return null;
  }).filter((page) => page !== null);
  /* Create the hierarchical structure */
  const rootPage = pagesData.find((page) => page.root === true);
  if (rootPage === undefined) {
    console.error('[MERGE STRUCTURE] Did not find the root page!');
    return null;
  }
  return createPageStructure(rootPage, pagesData);
}

/**
 * Copies the CXone Expert page thumbnail file from one page ("source") to another ("target"),
 * potentially cross-library.
 *
 * @param {object} source - Information about the source page.
 * @param {string} source.sourceLib - The internal library shortname/identifier of the source page.
 * @param {string} source.sourceID - The page identifier "number" of the source page.
 * @param {object} target - Information about the target page.
 * @param {string} target.targetLib - The internal library shortname/identifier of the target page.
 * @param {string} target.targetID - The page identifier "number" of the target page.
 * @returns {Promise<boolean>} True if thumbnail copy succeeded, false otherwise.
 */
async function copyPageThumbnail({ sourceLib, sourceID }, { targetLib, targetID }) {
  if (!isNonEmptyString(sourceLib) || !isNonEmptyString(sourceID)) {
    console.error(`[COPY THUMBNAIL] Invalid source information "${sourceLib}-${sourceID}".`);
    return false;
  }
  if (!isNonEmptyString(targetLib) || !isNonEmptyString(targetID)) {
    console.error(`[COPY THUMBNAIL] Invalid target information: "${targetLib}-${targetID}".`);
  }
  try {
    const srcReqHeaders = generateAPIRequestHeaders(sourceLib);
    const trgtReqHeaders = generateAPIRequestHeaders(targetLib);
    const sourceURL = `https://${sourceLib}.libretexts.org/@api/deki/pages/${sourceID}/files/=mindtouch.page%2523thumbnail`;
    const filesReq = await axiosInstance.get(sourceURL, { headers: srcReqHeaders, responseType: 'arraybuffer' });
    if (filesReq.status === 200 && filesReq.data) {
      const fileData = filesReq.data;
      const targetURL = `https://${targetLib}.libretexts.org/@api/deki/pages/${targetID}/files/=mindtouch.page%2523thumbnail`;
      await axiosInstance.put(
        targetURL,
        fileData,
        {
          headers: {
            ...trgtReqHeaders,
            'Content-Type': filesReq.headers['content-type'],
          },
        },
      );
    }
  } catch (e) {
    console.warn(`[COPY THUMBNAIL] Error occured copying thumbnail from "${sourceLib}-${sourceID}" to "${targetLib}-${targetID}".`);
  }
  return true; // ignore non-existant thumbnails or other errors
}

/**
 * Save's a page's properties to the target library using the CXone Expert API (best-effort).
 *
 * @param {object} trgtReqHeaders - Headers (and auth token) to pass to CXone Expert API
 *  requests (towards the target).
 * @param {string} targetLib - The internal library shortname/identifier of the target page.
 * @param {LibreTranslatedPage} page - The page information object.
 * @param {string} rootURL - The base URL of the library's pages API.
 * @param {string} newPageID - The identifier of the newly created library page to work on.
 * @returns {Promise<boolean>} Returns true if save attempted (errors are logged).
 */
async function savePageProperties(trgtReqHeaders, targetLib, page, rootURL, newPageID) {
  try {
    const reqTokenHeaders = trgtReqHeaders || generateAPIRequestHeaders(targetLib);
    let propEntries = '';
    const addPropEntry = (currEntries, { newKey, newValue }) => `
      ${currEntries}
      <property name="${newKey}">
        <contents type="text/plain; charset=utf-8;">${newValue}</contents>
      </property>
    `;
    if (Array.isArray(page.props)) {
      for (let i = 0, n = page.props.length; i < n; i += 1) {
        const currProp = page.props[i];
        let propValue = currProp.value;
        if (currProp?.name?.includes('idf.guideTabs')) {
          // handle escaped JSON
          propValue = JSON.stringify(JSON.parse(propValue));
        }
        propEntries = addPropEntry(propEntries, { newKey: currProp.name, newValue: propValue });
      }
    }
    if (isNonEmptyString(page.summary)) {
      propEntries = addPropEntry(propEntries, {
        newKey: 'mindtouch.page#overview',
        newValue: page.summary,
      });
    }
    if (isNonEmptyString(propEntries)) {
      const newPageProps = `<properties>${propEntries}</properties>`;
      const updatePropsRes = await axiosInstance.put(`${rootURL}${newPageID}/properties`, newPageProps, {
        headers: {
          ...reqTokenHeaders,
          'Content-Type': 'text/xml; charset=utf-8;',
        },
      });
      if (updatePropsRes.status !== 200) {
        throw (new Error(updatePropsRes));
      }
    }
  } catch (e) {
    console.warn(`[SAVE PAGE PROPS] Warning: Error saving properties to new page ${targetLib}-${newPageID}:`);
    console.warn(e);
  }
  return true;
}

/**
 * Saves a page's tags to the target library using the CXone Expert API (best-effort).
 *
 * @param {object} trgtReqHeaders - Headers (and auth token) to pass to CXone Expert API
 *  requests (towards the target).
 * @param {string} targetLib - The internal library shortname/identifier of the target page.
 * @param {LibreTranslatedPage} page - The page information object.
 * @param {string} rootURL - The base URL of the library's pages API.
 * @param {string} newPageID - The identifier of the newly created library page to work on.
 * @returns {Promise<boolean>} Returns true if save attempted (errors are logged).
 */
async function savePageTags(trgtReqHeaders, targetLib, page, rootURL, newPageID) {
  try {
    const reqTokenHeaders = trgtReqHeaders || generateAPIRequestHeaders(targetLib);
    const newPageTags = createTagsXML([...page.tags, `source[translate]-${page.lib}-${page.id}`]);
    const updateTagsRes = await axiosInstance.put(`${rootURL}${newPageID}/tags`, newPageTags, {
      headers: {
        ...reqTokenHeaders,
        'Content-Type': 'text/xml; charset=utf-8;',
      },
    });
    if (updateTagsRes.status !== 200) {
      throw (new Error(updateTagsRes));
    }
  } catch (e) {
    console.warn(`[SAVE PAGE TAGS] Warning: Error saving tags to new page ${targetLib}-${newPageID}:`);
    console.warn(e);
  }
  return true;
}

/**
 * Recursively saves a page and its subpages to the provided target LibreTexts library. Page paths
 *  are built up from the 'root' relative path using the parentPath parameter.
 *
 * @param {LibreTranslatedPage} page - The page to save to the target library.
 * @param {object} target - An object with information about where to save the content.
 * @param {string} target.targetLib - The LibreTexts library shortname to save the content to.
 * @param {string} target.targetPath - The root path to save the hierarchy under.
 * @param {string} [target.parentPath=''] - The relative path of the page's parent to
 *  prepend to the current page's path.
 * @returns {Promise<boolean>} True if save succeeded, false otherwise.
 */
async function saveToLibrary(page, { targetLib, targetPath, parentPath = '' }) {
  try {
    if (!page) {
      throw (new Error('Page data not provided or invalid.'));
    }
    if (!isNonEmptyString(targetLib)) {
      throw (new Error('Target library not provided or invalid.'));
    }
    if (!isNonEmptyString(targetPath)) {
      throw (new Error('Target path not provided or invalid.'));
    }
    console.log(`[SAVE TRANSLATED PAGE] ${page.lib}-${page.id}`);
    const root = `https://${targetLib}.libretexts.org/@api/deki/pages/`;
    const reqTokenHeaders = generateAPIRequestHeaders(targetLib);

    const trimTitle = page.title.trim();
    let newPagePath = trimTitle;
    let newPageURLTitle = trimTitle;
    if (isNonEmptyString(page.urlNumPrefix)) {
      const splitTitle = newPageURLTitle.split(':');
      if (splitTitle.length > 1) {
        newPageURLTitle = splitTitle[1].trim();
      }
      newPagePath = `${page.urlNumPrefix}: ${newPageURLTitle}`;
    }
    newPagePath = newPagePath.replaceAll(' ', '_');

    const pagePath = assembleUrl([targetPath, parentPath, newPagePath]);
    const finalPath = encodeURIComponent(encodeURIComponent(pagePath));
    const createPageRes = await axiosInstance.post(
      `${root}=${finalPath}/contents?title=${encodeURIComponent(trimTitle)}&edittime=now&abort=exists&dream.out.format=json`,
      page.contents.trim(),
      {
        headers: {
          ...reqTokenHeaders,
          'Content-Type': 'text/plain; charset=utf-8;',
        },
      },
    );
    const pageCreateData = createPageRes?.data;
    if (pageCreateData['@status'] !== 'success') {
      throw (new Error(pageCreateData));
    }
    await snooze(2 * ONE_SECOND);
    const newPageData = pageCreateData.page;
    const newPageID = newPageData['@id'];
    if (!isNonEmptyString(newPageID)) {
      throw (new Error('New PageID is missing or invalid.'));
    }
    await savePageTags(reqTokenHeaders, targetLib, page, root, newPageID);
    await snooze(2 * ONE_SECOND);
    await savePageProperties(reqTokenHeaders, targetLib, page, root, newPageID);
    await snooze(2 * ONE_SECOND);
    if (Array.isArray(page.subpages)) {
      await async.eachLimit(page.subpages, MAX_CONCURRENT, async (subpage) => {
        const subpathData = {
          parentPath: assembleUrl([parentPath, newPagePath]),
          targetLib,
          targetPath,
        };
        await saveToLibrary(subpage, subpathData);
      });
    }
    if (page.tags?.includes('article:topic-category') || page.tags?.includes('article:topic-guide')) {
      await snooze(ONE_SECOND);
      await copyPageThumbnail(
        { sourceLib: page.lib, sourceID: page.id },
        { targetID: newPageID, targetLib },
      );
    }
    return true;
  } catch (e) {
    console.error('[SAVE TRANSLATED PAGE] Error encountered while saving page:');
    console.error(JSON.stringify(e, null, 2));
    console.error(JSON.stringify(e.response?.data, null, 2));
  }
  return false;
}

/**
 * Main driver function for processing translated content and saving it to a LibreTexts library.
 *
 * @param {object} eventDetails - The Lambda trigger event details.
 * @returns {Promise<boolean>} True if process succeeded, false otherwise.
 */
async function processTranslated(eventDetails) {
  if (!isNonEmptyString(eventDetails.jobId)) {
    console.error('[PROCESS TRANSLATED] Initating job identifier found or invalid.');
    return false;
  }
  if (!axiosInstance) {
    axiosInstance = axios.create({
      httpsAgent: new https.Agent({ keepAlive: true }),
    });
  }

  const transOutputURI = await getTranslationDetailsURI(eventDetails.jobId);
  if (transOutputURI === null) {
    return false; // error logged in previous call
  }
  const s3Client = new S3Client({
    credentials: {
      accessKeyId: process.env.AWS_S3_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_S3_SECRET_ACCESS_KEY,
    },
    region: process.env.AWS_ENGINE_REGION,
  });
  const jobMetadata = await retrieveTranslationJobDetails(s3Client, transOutputURI);
  if (jobMetadata === null) {
    return false; // error logged in previous call
  }
  const { details, lib, id } = jobMetadata;
  const sourceCoverID = `${lib}-${id}`;
  const inputMetadata = await retrieveInputMetadata(
    s3Client,
    `${sourceCoverID}/${sourceCoverID}.metadata.json`,
  );
  if (inputMetadata === null) {
    return false; // error logged in previous call
  }

  const {
    lib: sourceLib,
    targetLib,
    targetPath,
    notifyAddrs,
  } = inputMetadata;
  sourceLibName = sourceLib;
  targetLibName = targetLib;
  const srcParams = await retrieveLibraryParameters(true);
  const trgtParams = await retrieveLibraryParameters();
  if (!srcParams || !trgtParams) {
    console.error('Fatal Error: Couldn\'t retrieve library keys.');
    return false;
  }

  const fileNames = details.map((f) => `${jobMetadata.outputDataPrefix}${f.targetFile}`);
  const translatedPages = await async.mapLimit(
    fileNames,
    MAX_CONCURRENT,
    async (file) => retrieveAndProcessTranslatedContent(s3Client, file),
  );
  const pageStructure = mergeInputStructure(inputMetadata, translatedPages);
  const saveSuccess = await saveToLibrary(pageStructure, {
    targetLib,
    targetPath,
  });
  if (saveSuccess) {
    await sendCompletionNotification(notifyAddrs, lib, id, targetLib, targetPath);
    console.log('[PROCESS TRANSLATED] Successfuly processed translated text.');
  } else {
    console.error('[PROCESS TRANSLATED] Error encountered saving translated text.');
  }
  return saveSuccess;
}

/**
 * Exposes the main function to the Lambda execution environment.
 *
 * @param {object} event - The event that triggered the Lambda invocation.
 * @returns {Promise<boolean>} True if process succeeded, false otherwise.
 */
export default async function translatedEventHandler(event) {
  if (!event || !event.detail || event.detail.jobStatus !== 'COMPLETED') {
    console.error('[HANDLER] Event information is invalid or missing.');
    return false;
  }
  return processTranslated(event.detail);
}
