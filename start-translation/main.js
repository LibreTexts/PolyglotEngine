/**
 * @file Defines the functions to gather content from a LibreTexts library and submit
 * a request to have it translated.
 * @author LibreTexts <info@libretexts.org>
 */
import crypto from 'crypto';
import { Buffer } from 'buffer';
import util from 'util';
import * as https from 'https';
import axios from 'axios';
import async from 'async';
import bluebird from 'bluebird';
// import * as cheerio from 'cheerio';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { SSMClient, GetParametersByPathCommand } from '@aws-sdk/client-ssm';
import { SQSClient, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { TranslateClient, StartTextTranslationJobCommand } from '@aws-sdk/client-translate';

const Promise = bluebird;
const LIBREBOT = 'LibreBot';
const ONE_SECOND = 1000;
const MAX_CONCURRENT = 2;
const ENGLISH_LANG_CODE = 'en';

let axiosInstance;
let sourceLibKey;
let sourceLibSecret;

/**
 * Object containing headers to use in calls to the CXone Expert API.
 *
 * @typedef {object} TokenHeaders
 * @property {string} X-Requested-With - CORS header to indicate request method.
 * @property {string} X-Deki-Token - A timestamped, encrypted token used to authenticate
 *  a provided user to the API.
 */

/**
 * Object containing information about a CXone Expert page's special properties.
 *
 * @typedef {object} PageProp
 * @property {string} name - The internal name of the property.
 * @property {string} value - The text value of the property.
 */

/**
 * An internal representation of a LibreText page and its metadata.
 *
 * @typedef {object} LibrePage
 * @property {string} lib - The internal LibreTexts library shortname/identifier.
 * @property {number} id - The page identifier number.
 * @property {string} url - The human-friendly URL of the page.
 * @property {string} title - The page's UI title.
 * @property {string[]} tags - An array of a page's tags and their values.
 * @property {LibrePage[]} subpages - An array of "subpages" underneath the page
 *  in the hierarchy (can be deeply nested).
 * @property {string} [contents] - The HTML contents of the page.
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
 * Generates HTTP request headers for use with the CXone (LibreText libraries) API.
 *
 * @returns {TokenHeaders} Headers to add to an Expert request.
 */
function generateAPIRequestHeaders() {
  const epoch = Math.floor(Date.now() / ONE_SECOND);
  const hmac = crypto.createHmac('sha256', sourceLibSecret);
  hmac.update(`${sourceLibKey}${epoch}=${LIBREBOT}`);
  return {
    'X-Requested-With': 'XMLHttpRequest',
    'X-Deki-Token': `${sourceLibKey}_${epoch}_=${LIBREBOT}_${hmac.digest('hex')}`,
  };
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
 * Extracts a page's tag values from information retreived from the CXone API.
 *
 * @param {object} tagsObj - An object containing page tags information.
 * @returns {string[]} The page's tag values.
 */
function processTags(tagsObj) {
  const processed = [];

  const processSingle = (singleTag) => {
    if (typeof (singleTag['@value']) === 'string') {
      processed.push(singleTag['@value']);
    }
  };

  if (typeof (tagsObj) === 'object') {
    if (Array.isArray(tagsObj.tag) && tagsObj.tag.length > 0) {
      for (let i = 0, n = tagsObj.tag.length; i < n; i += 1) {
        processSingle(tagsObj.tag[i]);
      }
    } else if (typeof (tagsObj.tag) === 'object') { // single tag
      processSingle(tagsObj.tag);
    }
  }
  return processed;
}

/**
 * Attempts to determine a page's section numbering from the URL in order to preserve
 * it upon recreation.
 * 
 * @param {string} path - The path of the current page relative to the library's hostname.
 * @returns {[boolean, (string|undefined), (string|undefined)]} A 3-tuple containing: success flag,
 *  section number URL prefix (if found), extracted section title, if found.
 */
function parsePagePath(path) {
  if (typeof (path) === 'string') {
    const splitPath = path.split('/');
    const relativePath = splitPath[splitPath.length - 1];
    const splitPagePath = relativePath.split('%3A');
    const numberPart = splitPagePath[0];
    if (splitPagePath.length > 1 && /(\d|zz)/.test(numberPart)) { // 'zz' back matter numbering
      const titlePart = splitPagePath[1]?.replaceAll('_', ' ').trim();
      return [true, numberPart, titlePart];
    }
  }
  return [false];
}

/**
 * Retrieves the CXone Expert key-secret pair for a given library and initializes the
 * global variables for use with the Expert API.
 *
 * @param {string} lib - The internal library shortname/identifier.
 * @returns {Promise<boolean>} True if successful, false if error encountered.
 */
async function retrieveLibraryParameters(lib) {
  try {
    console.log(`[RETRIEVE PARAMS] ${lib}`);
    const sysClient = new SSMClient();
    const keysResponse = await sysClient.send(new GetParametersByPathCommand({
      Path: process.env.AWS_SSM_LIB_SERVERKEYS_PATH,
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
    sourceLibKey = libraryKey.Value;
    sourceLibSecret = librarySec.Value;
    return true;
  } catch (e) {
    console.error('[RETRIEVE PARAMS] Error retrieving library keys:');
    console.error(e);
  }
  return false;
}

/**
 * Deletes a message from the engine's queue.
 *
 * @param {string} receiptHandle - The receipt handle of the message to delete.
 * @returns {Promise<boolean>} True if deleted successfully, false otherwise.
 */
async function deleteQueueMessage(receiptHandle) {
  const sqsClient = new SQSClient();
  const deleteRes = await sqsClient.send(new DeleteMessageCommand({
    ReceiptHandle: receiptHandle,
    QueueUrl: process.env.AWS_SQS_QUEUE_URL,
  }));
  if (deleteRes?.$metadata?.httpStatusCode !== 200) {
    console.error('[DELETE QUEUE MSG] Message could\'t be deleted!');
    return false;
  }
  return true;
}

/**
 * Recursively retrieves a page's subpages from the CXone API.
 *
 * @param {string} lib - The LibreTexts library subdomain.
 * @param {string} path - The relative path to the library domain.
 * @param {string} [parent=null] The current page's parent ID number.
 * @returns {Promise<object>} Information about the page, including its subpages.
 */
async function retrieveSubpages(lib, path, parent = null) {
  console.log(`[RETRIEVE] ${lib}, ${path}`);
  try {
    const root = `https://${lib}.libretexts.org/@api/deki/pages/`;
    const reqTokenHeaders = generateAPIRequestHeaders();
    const pageInfoRes = await axiosInstance.get(`${root}=${encodeURIComponent(encodeURIComponent(path))}?dream.out.format=json`, {
      headers: reqTokenHeaders,
    });
    const pageInfo = pageInfoRes.data;
    const subpagesRes = await axiosInstance.get(`${root}${pageInfo['@id']}/subpages?limit=all&dream.out.format=json`, {
      headers: reqTokenHeaders,
    });
    const subpages = subpagesRes.data;
    await snooze(15 * ONE_SECOND);

    /* Keep walking the tree */
    const subpagePromises = [];
    const addSubpage = (currentSubpage) => {
      const [subLib, subPath] = parseURL(currentSubpage['uri.ui']);
      subpagePromises.push(retrieveSubpages(subLib, subPath, pageInfo['@id']));
    };
    if (Array.isArray(subpages['page.subpage'])) {
      for (let i = 0, n = subpages['page.subpage'].length; i < n; i += 1) {
        addSubpage(subpages['page.subpage'][i]);
      }
    } else if (typeof (subpages['page.subpage']) === 'object') { // single subpage
      addSubpage(subpages['page.subpage']);
    }
    await snooze(5 * ONE_SECOND);
    const foundSubpages = await Promise.all(subpagePromises);

    const [couldParse, sectionNum, sectionTitle] = parsePagePath(path);
    if (pageInfo && subpages) {
      let infoObj = {
        id: pageInfo['@id'],
        url: pageInfo['uri.ui'],
        title: pageInfo.title,
        tags: processTags(pageInfo.tags),
        subpages: foundSubpages,
        lib,
        path,
        ...(couldParse && {
          urlNumPrefix: sectionNum,
          urlTitleExtract: sectionTitle,
        }),
      };
      if (parent) {
        infoObj = { ...infoObj, root: false, parent };
      } else {
        infoObj = { ...infoObj, root: true, parent: null };
        if (!infoObj.tags.includes('coverpage:yes')) {
          throw (new Error(`Polyglot Engine Warning: Provided URL is not a coverpage.
            Translation of more than one text at a time has been disabled.`));
        }
      }
      return infoObj;
    }
  } catch (e) {
    console.error(e);
  }
  return null;
}

/**
 * Retrieves a page's properties from the CXone Expert API.
 *
 * @param {LibrePage} page - A page information object.
 * @param {TokenHeaders} tokenHeaders - Headers (auth token) to pass to the API.
 * @returns {Promise<object[]>} An array of page property information objects.
 */
async function getPageProperties(page, tokenHeaders) {
  const addProperty = (arr, prop) => {
    const propName = prop['@name'];
    const propVal = prop?.contents['#text'];
    if (isNonEmptyString(propName) && isNonEmptyString(propVal)) {
      arr.push({
        name: propName,
        value: propVal,
      });
    }
  };
  let foundProps = [];
  const currPage = page;
  if (typeof (currPage) !== 'object') {
    console.error('[PAGE PROPS] Invalid object provided.');
    return foundProps;
  }
  console.log(`[PAGE PROPS] ${page.lib}-${page.id}`);
  try {
    const reqHeaders = tokenHeaders || generateAPIRequestHeaders();
    const propsUrl = `https://${page.lib}.libretexts.org/@api/deki/pages/${page.id}/properties?dream.out.format=json`;
    const propsRes = await axiosInstance.get(propsUrl, {
      headers: reqHeaders,
    });
    const propsData = propsRes.data;
    const pageProps = propsData?.property;

    if (Array.isArray(pageProps)) {
      for (let i = 0, n = pageProps.length; i < n; i += 1) {
        const currProp = pageProps[i];
        addProperty(foundProps, currProp);
      }
    } else if (typeof (pageProps) === 'object' && pageProps !== null) {
      addProperty(foundProps, pageProps);
    }
    foundProps = foundProps.filter((prop) => !prop?.name?.includes('editedby'));
    foundProps = foundProps.filter((prop) => prop !== null && prop !== undefined);
  } catch (e) {
    console.error('[PAGE PROPS] Error retrieving or processing page properties:');
    console.error(e);
  }
  return foundProps;
}

/**
 * Retrieves the content for a given page from the CXone Expert API.
 *
 * @param {LibrePage} page - A page information object.
 * @param {TokenHeaders} tokenHeaders - Headers (auth token) to pass to the API.
 * @returns {Promise<string>} The page's contents as a string containing HTML.
 */
async function getPageContent(page, tokenHeaders) {
  if (typeof (page) === 'object') {
    console.log(`[CONTENT] ${page.lib}-${page.id}`);
    try {
      const reqHeaders = tokenHeaders || generateAPIRequestHeaders();
      const pageContentRes = await axiosInstance.get(`https://${page.lib}.libretexts.org/@api/deki/pages/${page.id}/contents?mode=edit&format=html&dream.out.format=json`, {
        headers: reqHeaders,
      });
      return pageContentRes?.data?.body;
    } catch (e) {
      console.error(e);
    }
  }
  return null;
}

/**
 * Recursively retrieves a page's contents and the contents of all its subpages.
 *
 * @param {LibrePage} page - A page information object.
 * @param {TokenHeaders} tokenHeaders - Headers (auth token) to pass to the CXone Expert API.
 * @returns {Promise<LibrePage>} The page information object with contents and updated subpages.
 */
async function getSubpageContents(page, tokenHeaders) {
  const currPage = page;
  const reqHeaders = tokenHeaders || generateAPIRequestHeaders();
  if (typeof (currPage) === 'object') {
    currPage.contents = await getPageContent(currPage, reqHeaders);
    const pageProps = await getPageProperties(currPage, reqHeaders);
    if (Array.isArray(pageProps)) {
      const summaryIdx = pageProps.findIndex((prop) => prop.name === 'mindtouch.page#overview');
      if (summaryIdx > -1) {
        currPage.summary = pageProps[summaryIdx].value;
        pageProps.splice(summaryIdx, 1);
      }
      currPage.props = pageProps;
    }
    if (Array.isArray(currPage.subpages)) {
      currPage.subpages = await async.mapLimit(
        currPage.subpages,
        MAX_CONCURRENT,
        async (subpage) => getSubpageContents(subpage, reqHeaders),
      );
    }
  }
  return currPage;
}

/*
function processElement($, elem) {
  const mathInlineRegex = /\\\(.*?(?<!\\\\)\\\)/gmi;
  const mathDisplayRegex = /\\\[.*?(?<!\\\\)\\\]/gmi;
  let elemHtml = $(elem).html();
  elemHtml = elemHtml.replaceAll(mathInlineRegex, (match) =>
  `<span translate='no'>${match}</span>`);
  elemHtml = elemHtml.replaceAll(mathDisplayRegex, (match) =>
  `<span translate='no'>${match}</span>`);
  $(elem).html(elemHtml);
  if ($(elem).children().length > 0) {
    $(elem).children().each((_i, child) => processElement($, child));
  }
}
*/

/**
 * Recursively performs pre-processing on the page's (and subpages') contents.
 *
 * @param {LibrePage} page - A page information object.
 * @returns {Promise<LibrePage>} The page with updated contents.
 */
async function processPageContents(page) {
  const pageData = page;
  if (typeof (pageData?.contents) === 'string') {
    console.log(`[PROCESS CONTENTS] ${page.lib}-${page.id}`);
    try {
      let { contents } = pageData;
      contents = `
        <span
          data-libre-pagetitle="true"
          data-libre-lib="${page.lib}"
          data-libre-pageid="${page.id}"
        >
          ${page.title}
        </span>
        ${(typeof (page.summary) === 'string') ? `<p data-libre-pagesummary="true">${page.summary}</p>` : ''}
        ${contents}
      `;
      const dekiRegex = /{{([A-Za-z.()]*)}}/gmi;
      const mathInlineRegex = /\\\(.*?(?<!\\\\)\\\)/gmi;
      const mathDisplayRegex = /\\\[.*?(?<!\\\\)\\\]/gmi;
      const noTranslateDiv = (match) => `<div translate="no">${match}</div>`;
      const noTranslateSpan = (match) => `<span translate="no">${match}</span>`;
      contents = contents.replaceAll(dekiRegex, noTranslateDiv);
      contents = contents.replaceAll(mathInlineRegex, noTranslateSpan);
      contents = contents.replaceAll(mathDisplayRegex, noTranslateSpan);
      /*
      $('body').html(bodyHTML.trim());
      const $ = cheerio.load(contents, { decodeEntities: true });
      let bodyHTML = $('body').html();
      if (bodyHTML) {
        const dekiRegex = /{{([A-Za-z.()]*)}}/gmi;
        const mathInlineRegex = /\\\(.*?(?<!\\\\)\\\)/gmi;
        const mathDisplayRegex = /\\\[.*?(?<!\\\\)\\\]/gmi;
        const noTranslateDiv = (match) => `<div translate="no">${match}</div>`;
        const noTranslateSpan = (match) => `<span translate="no">${match}</span>`;
        bodyHTML = bodyHTML.replaceAll(dekiRegex, noTranslateDiv);
        bodyHTML = bodyHTML.replaceAll(mathInlineRegex, noTranslateSpan);
        bodyHTML = bodyHTML.replaceAll(mathDisplayRegex, noTranslateSpan);
        $('body').html(bodyHTML.trim());
      }
      $('body').children().each((_i, elem) => processElement($, elem));
      console.log('DONE PROCESS');
      $('*').each((_idx, elem) => {
        const elemHtml = $(elem).html();
        const dekiRegex = /{{([A-Za-z.()]*)}}/gmi;
        if (dekiRegex.test(elemHtml)) {
          $(elem).attr('translate', 'no');
        }
      });
      $('p,span,div').each((_idx, elem) => {
        const mathInlineRegex = /\\\(.*?(?<!\\\\)\\\)/gmi;
        const mathDisplayRegex = /\\\[.*?(?<!\\\\)\\\]/gmi;
        let elemHtml = $(elem).html();
        elemHtml = elemHtml.replaceAll(mathInlineRegex,
          (match) => `<span translate='no'>${match}</span>`);
        elemHtml = elemHtml.replaceAll(mathDisplayRegex,
          (match) => `<span translate='no'>${match}</span>`);
        $(elem).html(elemHtml);
      });
      $('pre').each((_idx, elem) => {
        $(elem).attr('translate', 'no');
      });
      */
      pageData.contents = contents;
    } catch (e) {
      console.error(`[PROCESS CONTENTS] Error processing ${pageData?.lib}-${pageData?.id}:`);
      console.error(e);
    }
  }
  if (Array.isArray(pageData.subpages)) {
    pageData.subpages = await async.mapLimit(
      pageData.subpages,
      MAX_CONCURRENT,
      processPageContents,
    );
  }
  return pageData;
}

/**
 * Recursively flattens a page hierarchy and removes unnecessary information, like page contents.
 *
 * @param {LibrePage} page - A page information object.
 * @returns {LibrePage[]} The flattened array without page contents.
 */
function createFlatPageMetadataList(page) {
  let pagesArr = [];
  const pageData = page;
  if (Array.isArray(pageData.subpages)) {
    for (let i = 0, n = pageData.subpages.length; i < n; i += 1) {
      pagesArr = [...pagesArr, ...createFlatPageMetadataList(pageData.subpages[i])];
    }
  }
  delete pageData.contents;
  delete pageData.subpages;
  pagesArr.unshift(pageData); // prepend current to preserve "ordering"
  return pagesArr;
}

/**
 * Recursively creates AWS S3 upload commands for a page hierachy's contents.
 *
 * @param {LibrePage} page - A page information object.
 * @param {string} [directory] - The parent directory path to prepend to the file key, if not root.
 * @returns {Promise<PutObjectCommand>[]} An array of thenable S3 PutObjectCommand's.
 */
function createUploadCommands(page, directory) {
  let commands = [];
  let dirRoot = '';
  if (page.root === true) {
    dirRoot = `${page.lib}-${page.id}`;
  } else if (typeof (directory) === 'string') {
    dirRoot = directory;
  } else {
    throw (new Error('nodirectory'));
  }
  const fileKey = `${dirRoot}/${page.lib}-${page.id}.html`;
  commands.push(new PutObjectCommand({
    Bucket: process.env.AWS_S3_INPUT_BUCKET,
    Key: fileKey,
    Body: Buffer.from(page.contents),
  }));
  if (Array.isArray(page.subpages)) {
    for (let i = 0, n = page.subpages.length; i < n; i += 1) {
      commands = [
        ...commands,
        ...createUploadCommands(page.subpages[i], dirRoot),
      ];
    }
  }
  return commands;
}

/**
 * Uploads a LibreText's content and relevant metadata to AWS S3.
 *
 * @param {LibrePage} page - A page information object.
 * @param {object} target - An object with information about where to place the translated text.
 * @param {string} target.targetLib - The LibreTexts library shortname to save the
 *  translated text to.
 * @param {string} target.targetPath - The path to save the translated text under
 *  (relative to targetLib).
 * @returns {Promise<boolean>} Whether the upload(s) succeeded.
 */
async function uploadLibreText(page, { targetLib, targetPath }) {
  if (page === null || typeof (page) !== 'object') return false;
  let uploadSuccess = true;
  const s3Client = new S3Client({
    credentials: {
      accessKeyId: process.env.AWS_S3_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_S3_SECRET_ACCESS_KEY,
    },
    region: process.env.AWS_ENGINE_REGION,
  });
  try {
    const uploadCommands = createUploadCommands(page);
    const uploadResponses = await async.map(uploadCommands, async (comm) => s3Client.send(comm));
    let failCount = 0;
    for (let i = 0, n = uploadResponses.length; i < n; i += 1) {
      if (uploadResponses[i].$metadata?.httpStatusCode !== 200) {
        failCount += 1;
      }
    }
    if (failCount === 0) {
      const rootID = `${page.lib}-${page.id}`;
      /* Upload page structure to use in post-processing */
      const flatMetadata = createFlatPageMetadataList(page);
      const rootData = {
        lib: page.lib,
        id: page.id,
        pageCount: flatMetadata.length,
        uploaded: new Date().toISOString(),
        allPages: flatMetadata,
        targetLib,
        targetPath,
      };
      await s3Client.send(new PutObjectCommand({
        Bucket: process.env.AWS_S3_OUTPUT_BUCKET,
        Key: `${rootID}/${rootID}.metadata.json`,
        Body: Buffer.from(JSON.stringify(rootData)),
      }));
    } else {
      console.error(`[UPLOAD CONTENT] ${failCount} pages failed to upload.`);
      uploadSuccess = false;
    }
  } catch (e) {
    console.error('[UPLOAD CONTENT] Error uploading content or metadata:');
    console.error(e);
    uploadSuccess = false;
  }
  return uploadSuccess;
}

/**
 * Submits a Batch Translation Job request to the AWS Translate API.
 * For language codes, see {@link https://docs.aws.amazon.com/translate/latest/dg/what-is-languages.html}
 *
 * @param {string} coverID - The lib-ID format identifier of the root page.
 * @param {string} outLangCode - The target language code of the desired output language.
 * @returns {Promise<boolean>} True if job was successfully submitted, false otherwise.
 */
async function initiateTranslationJob(coverID, outLangCode) {
  if (typeof (outLangCode) !== 'string' || outLangCode.length < 2) {
    console.error('[START TRANSLATION JOB] Target language code not provided.');
    return false;
  }
  let startSuccess = true;
  const transClient = new TranslateClient({
    credentials: {
      accessKeyId: process.env.AWS_TRANS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_TRANS_SECRET_ACCESS_KEY,
    },
    region: process.env.AWS_ENGINE_REGION,
  });
  try {
    const transReqResponse = await transClient.send(new StartTextTranslationJobCommand({
      DataAccessRoleArn: process.env.AWS_TRANS_ROLE_ARN,
      InputDataConfig: {
        ContentType: 'text/html',
        S3Uri: `s3://${process.env.AWS_S3_INPUT_BUCKET}/${coverID}/`,
      },
      JobName: coverID,
      OutputDataConfig: {
        S3Uri: `s3://${process.env.AWS_S3_OUTPUT_BUCKET}/${coverID}/`,
      },
      SourceLanguageCode: ENGLISH_LANG_CODE,
      TargetLanguageCodes: [outLangCode],
    }));
    if (transReqResponse.$metadata?.httpStatusCode !== 200) {
      startSuccess = false;
    }
  } catch (e) {
    console.error('[START TRANSLATION JOB] Error submitting job:');
    console.error(e);
    startSuccess = false;
  }
  return startSuccess;
}

/**
 * Intiates the Polyglot Engine's translation functions by retreiving a LibreText's contents,
 * processing them, and uploading them to AWS S3.
 *
 * @param {object} event - The initial Lambda invocation event.
 * @returns {Promise<boolean>} Returns true upon completion (even if errors occured).
 */
async function startTranslation(event) {
  if (!axiosInstance) {
    axiosInstance = axios.create({
      httpsAgent: new https.Agent({ keepAlive: true }),
    });
  }

  let reqParams;
  try {
    reqParams = JSON.parse(event.body);
  } catch (e) {
    console.error('[START TRANSLATION] Error parsing queue message:');
    console.error(e);
  }
  console.log(`[TRANSLATING] ${reqParams.lib}/${reqParams.path}`);

  const msgDeleted = await deleteQueueMessage(event.receiptHandle);
  if (!msgDeleted) {
    return false; // error logged in previous call
  }

  const paramsRetrieve = await retrieveLibraryParameters(reqParams.lib);
  if (!paramsRetrieve) {
    console.error('Fatal Error: Couldn\'t retrieve library keys.');
    return false;
  }

  let subpageResults = await retrieveSubpages(reqParams.lib, reqParams.path);
  if (subpageResults === null) {
    console.error('[START TRANSLATION] Encountered an error retreiving page information.');
    return false;
  }

  console.log('[PAGE SEARCH] Finished discovering pages.');
  console.log('[RATE LIMIT PROTECTION] Snoozing for thirty seconds...');
  await snooze(30 * ONE_SECOND);

  console.log('[PAGE CONTENT] Retrieving page contents...');
  subpageResults = await getSubpageContents(subpageResults);
  console.log('[PAGE CONTENTS] Finished retrieving page contents...');
  console.log(util.inspect(subpageResults, false, 10, true));

  console.log('[PROCESS CONTENTS] Pre-processing page contents....');
  subpageResults = await processPageContents(subpageResults);
  console.log('[PROCESS CONTENTS] Finished content pre-processing.');

  console.log('[LIBRETEXT UPLOAD] Uploading page contents to S3...');
  await uploadLibreText(subpageResults, {
    targetLib: reqParams.targetLib,
    targetPath: reqParams.targetPath,
  });
  console.log('[LIBRETEXT UPLOAD] Finished uploading content to S3.');

  console.log('[START TRANSLATION JOB] Submitting translation job...');
  const trnsJob = await initiateTranslationJob(
    `${subpageResults.lib}-${subpageResults.id}`,
    reqParams.language,
  );
  if (trnsJob) {
    console.log('[START TRANSLATION JOB] Translation job submitted successfully.');
  }

  console.log(`[COMPLETE] StartTranslation is complete. ${trnsJob ? 'Content translation will start shortly.' : ''}`);
  return true;
}

/**
 * Exposes the main function to the Lambda execution environment.
 *
 * @param {object} event - The event that triggered the Lambda invocation.
 * @returns {Promise<boolean>} True if process succeeed, false otherwise.
 */
export default async function startTranslationEventHandler(event) {
  console.log(event);
  if (!event || !event.Records || event.Records.length === 0) {
    console.error('[HANDLER] Event information is invalid or missing.');
    return false;
  }
  return startTranslation(event.Records[0]);
}
