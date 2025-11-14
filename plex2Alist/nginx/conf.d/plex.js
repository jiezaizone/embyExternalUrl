// author: @bpking  https://github.com/bpking1/embyExternalUrl
// 查看日志: "docker logs -f -n 10 plex-nginx 2>&1  | grep js:"
// 正常情况下此文件所有内容不需要更改

import config from "./constant.js";
import util from "./common/util.js";
import urlUtil from "./common/url-util.js";
import events from "./common/events.js";
import ngxExt from "./modules/ngx-ext.js";

const xml = require("xml");

/**
 * 检查 URL 是否是 MS API URL
 * @param {String} url - 要检查的 URL
 * @param {String} msAddr - MS 服务地址
 * @param {String} msPublicAddr - MS 公网地址
 * @returns {Boolean} 是否是 MS API URL
 */
function isMsApiUrl(url, msAddr, msPublicAddr) {
  if (!url) return false;
  return (msAddr && url.startsWith(msAddr)) || (msPublicAddr && url.startsWith(msPublicAddr));
}

/**
 * 处理 MS API URL 请求，获取重定向链接
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} url - MS API URL
 * @param {String} ua - User-Agent
 * @param {Number} timeout - 超时时间（毫秒），默认 30000
 * @returns {Promise<String|null>} 重定向链接，失败返回 null
 */
async function fetchMsApiRedirect(r, url, ua, timeout) {
  // NJS 不支持默认参数，在函数内部处理
  if (timeout === undefined || timeout === null) {
    timeout = 30000;
  }
  
  // 构造缓存键
  const cacheKey = `msapi:${url}:${ua}`;
  
  // 检查缓存
  const cachedResult = ngx.shared.routeL1Dict.get(cacheKey);
  if (cachedResult) {
    r.warn(`fetchMsApiRedirect: hit cache for ${url}, returning cached result`);
    return cachedResult;
  }
  
  r.warn(`fetchMsApiRedirect: requesting ${url}, timeout: ${timeout}ms`);
  const start = Date.now();
  try {
    const response = await ngx.fetch(url, {
      method: "GET",
      headers: {
        "User-Agent": ua,
      },
      max_response_body_size: 65535,
      timeout: timeout
    });
    const end = Date.now();
    r.warn(`fetchMsApiRedirect: response status: ${response.status}, cost: ${end - start}ms`);
    r.warn(`fetchMsApiRedirect full response: ${JSON.stringify(response)}`);

    // 处理302/301重定向响应，获取Location头中的URL
    if (response.status === 302 || response.status === 301) {
      const location = response.headers["Location"];
      if (location) {
        r.warn(`MS API returned redirect to: ${location}`);
        // 只有成功的结果才缓存，缓存时间5分钟
        util.dictAdd("routeL1Dict", cacheKey, location, 5 * 60 * 1000);
        return location;
      } else {
        r.warn(`MS API returned ${response.status} without Location header`);
        return null;
      }
    } else if (response.ok) {
      // 处理 JSON 响应（用于 strm302 API）
      try {
        const result = await response.json();
        r.warn(`fetchMsApiRedirect: JSON response: ${JSON.stringify(result)}`);
        if (result && result.url) {
          r.warn(`MS API returned URL from JSON: ${result.url}`);
          // 只有成功的结果才缓存，缓存时间5分钟
          util.dictAdd("routeL1Dict", cacheKey, result.url, 5 * 60 * 1000);
          return result.url;
        } else {
          r.warn(`MS API JSON response missing url field`);
          return null;
        }
      } catch (jsonError) {
        r.error(`MS API JSON parse failed: ${jsonError}`);
        return null;
      }
    } else {
      r.warn(`MS API returned status ${response.status}, not 200/301/302`);
      return null;
    }
  } catch (error) {
    r.error(`MS API request failed: ${error}`);
    return null;
  }
}

/**
 * 主函数：处理Plex重定向到直链
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 */
async function redirect2Pan(r) {
  // 优化：减少日志输出，仅在调试时启用
  events.njsOnExit(`redirect2Pan: ${r.uri}`);
  // r.warn(`redirect2Pan headersIn: ${JSON.stringify(r.headersIn)}`);
  // r.warn(`redirect2Pan args: ${JSON.stringify(r.args)}`);
  // r.warn(`redirect2Pan remote_addr: ${r.variables.remote_addr}`);

  // 检查是否允许重定向
  if (!allowRedirect(r)) {
    return internalRedirect(r);
  }

  const ua = r.headersIn["User-Agent"];
  r.warn(`redirect2Pan, UA: ${ua}`);

  // 检查转码设置
  if (config.transcodeConfig.enable
    && r.uri.toLowerCase().includes("/transcode/universal/start")
    && r.args.directPlay === "0") {
    r.warn(`required plex clients self report, skip modify`);
    return internalRedirect(r);
  }

  // 检查路由缓存
  const routeCacheConfig = config.routeCacheConfig;
  if (routeCacheConfig && routeCacheConfig.enable) {
    // webClient download only have itemId on pathParam
    let cacheKey = util.parseExpression(r, routeCacheConfig.keyExpression) ?? r.uri;
    r.log(`redirect2Pan routeCacheKey: ${cacheKey}`);
    // 优化：提前检查是否需要 UA 隔离
    const domainArr115 = config.strHead["115"];
    const needUaIsolation = Array.isArray(domainArr115) 
      ? domainArr115.some(d => cacheKey.includes(d)) 
      : (domainArr115 && cacheKey.includes(domainArr115));
    
    for (let index = 1; index < 3; index++) {
      const routeDictKey = `routeL${index}Dict`;
      let cachedLink = ngx.shared[routeDictKey].get(cacheKey);
      if (!cachedLink && needUaIsolation) {
        // 115必须使用UA区分缓存
        cachedLink = ngx.shared[routeDictKey].get(`${cacheKey}:${ua}`);
      }
      if (cachedLink) {
        r.warn(`hit cache ${routeDictKey}: ${cachedLink}`);
        if (cachedLink.startsWith("@")) {
          // 使用原始链接
          return internalRedirect(r, cachedLink, routeDictKey);
        } else {
          return redirect(r, cachedLink, routeDictKey);
        }
      }
    }
  }

  const fallbackUseOriginal = config.fallbackUseOriginal ?? true;
  // 提前定义MS相关配置变量，避免重复访问（提前到这里，以便尽早使用）
  const msAddr = config.msAddr;
  const msPublicAddr = config.msPublicAddr;
  const msApiKey = config.msApiKey;
  
  // 获取Plex文件路径
  const itemInfo = await util.cost(getPlexItemInfo, r);
  let mediaServerRes;
  if (itemInfo.filePath) {
    mediaServerRes = { path: itemInfo.filePath };
    r.warn(`get filePath from cache partInfoDict`);
  } else {
    r.warn(`itemInfoUri: ${itemInfo.itemInfoUri}`);
    mediaServerRes = await util.cost(fetchPlexFilePath,
      itemInfo.itemInfoUri,
      itemInfo.mediaIndex,
      itemInfo.partIndex
    );
    r.log(`mediaServerRes: ${JSON.stringify(mediaServerRes)}`);
    if (mediaServerRes.message.startsWith("error")) {
      r.error(`fail to fetch fetchPlexFilePath: ${mediaServerRes.message},fallback use original link`);
      return fallbackUseOriginal ? internalRedirect(r) : r.return(500, mediaServerRes.message);
    }
  }

  // strm file internal text maybe encode
  const notLocal = util.checkIsStrmByPath(mediaServerRes.path);
  r.warn(`notLocal: ${notLocal}`);
  if (notLocal) {
    const filePathPart = urlUtil.getFilePathPart(mediaServerRes.path);
    if (filePathPart) {
      // 需要小心处理filePathPart的编码，其他部分不要编码
      r.warn(`is CloudDrive/AList link, decodeURIComponent filePathPart before: ${mediaServerRes.path}`);
      mediaServerRes.path = mediaServerRes.path.replace(filePathPart, decodeURIComponent(filePathPart));
    } else {
      // 优化：减少不必要的日志
      // r.warn(`not is CloudDrive/AList link, decodeURIComponent filePath before: ${mediaServerRes.path}`);
      mediaServerRes.path = decodeURIComponent(mediaServerRes.path);
    }
  }

  // 优化：提前检查 MS API URL，如果是则跳过符号链接检查等后续步骤
  if (isMsApiUrl(mediaServerRes.path, msAddr, msPublicAddr)) {
    r.warn(`mediaServerRes.path is MS API URL, processing directly before symlink check`);
    const redirectUrl = await fetchMsApiRedirect(r, mediaServerRes.path, ua, 30000);
    if (redirectUrl) {
      r.warn(`fetchMsApiRedirect success, redirecting to: ${redirectUrl}`);
      return redirect(r, redirectUrl);
    } else {
      r.warn(`fetchMsApiRedirect failed, will continue with symlink check`);
    }
  }

  // 检查符号链接规则
  const symlinkRule = config.symlinkRule;
  if (symlinkRule && symlinkRule.length > 0) {
    const hitRule = symlinkRule.find(rule => util.strMatches(rule[0], mediaServerRes.path, rule[1]));
    if (hitRule) {
      r.warn(`hit symlinkRule: ${JSON.stringify(hitRule)}`);
      const realpath = util.checkAndGetRealpathSync(mediaServerRes.path);
      if (realpath) {
        r.warn(`symlinkRule realpath overwrite pre: ${mediaServerRes.path}`);
        mediaServerRes.path = realpath;
      }
    }
  }
  r.warn(`mount plex file path: ${mediaServerRes.path}`);

  // 添加表达式上下文到r对象
  // 因为Plex PartInfo缓存只有路径，暂时未实现
  r[util.ARGS.rXMediaKey] = mediaServerRes.media;
  // 优化：减少不必要的日志
  // ngx.log(ngx.WARN, `add plex Media to r`);
  // 路由规则处理，不必在mediaPathMapping之前，之前处理更简单，可以忽略mediaPathMapping
  const routeMode = util.getRouteMode(r, mediaServerRes.path, false, notLocal);
  const apiType = r.variables.apiType ?? "";
  // 优化：仅在调试时输出
  // r.warn(`getRouteMode: ${routeMode}, apiType: ${apiType}`);
  if (util.ROUTE_ENUM.proxy === routeMode) {
    return internalRedirect(r); // 使用原始链接
  } else if ((routeMode === util.ROUTE_ENUM.block)
    || (routeMode === util.ROUTE_ENUM.blockDownload && apiType.endsWith("Download"))
    || (routeMode === util.ROUTE_ENUM.blockPlay && apiType.endsWith("Play"))
    // Infuse使用VideoStreamPlay下载，UA不同，忽略apiType
    || (routeMode === util.ROUTE_ENUM.blockDownload && ua.includes("Infuse"))
  ) {
    return blocked(r);
  }
  
  // strm支持处理
  if (notLocal) {
    const strmInnerText = await util.cost(fetchStrmInnerText, r);
    // 检查是否返回了错误信息
    if (strmInnerText.startsWith("error")) {
      r.error(`fetchStrmInnerText failed: ${strmInnerText}`);
      // 如果获取STRM内容失败，使用原始链接
      return internalRedirect(r);
    }
    
    r.warn(`fetchStrmInnerText cover mount plex file path: ${strmInnerText}`);
    mediaServerRes.path = strmInnerText;
    
    // 如果STRM文件内容已经是MS API的URL，直接处理它
    if (isMsApiUrl(strmInnerText, msAddr, msPublicAddr)) {
      r.warn(`STRM content is already MS API URL, processing directly`);
      const redirectUrl = await fetchMsApiRedirect(r, strmInnerText, ua, 30000);
      if (redirectUrl) {
        r.warn(`fetchMsApiRedirect success, redirecting to: ${redirectUrl}`);
        return redirect(r, redirectUrl);
      } else {
        r.warn(`fetchMsApiRedirect failed or returned null, will continue with original MS API URL`);
        // 如果请求失败，继续后续处理，但不要再次处理 MS API URL
        // 设置一个标记，避免后续重复处理
        mediaServerRes.path = strmInnerText;
        mediaServerRes.skipMsApiCheck = true;
        
        // 在这里添加额外的处理逻辑，尝试其他方式获取直链
        r.warn(`Trying fallback method to get direct link`);
        const fallbackUrl = await tryFallbackMethods(r, strmInnerText, ua);
        if (fallbackUrl) {
          r.warn(`Fallback method success, redirecting to: ${fallbackUrl}`);
          return redirect(r, fallbackUrl);
        }
      }
    }
  }

  // 文件路径映射
  const mediaPathMapping = config.mediaPathMapping.slice(); // 警告：config.XX对象是当前VM共享变量
  // 优化：使用 forEach 替代 map，因为我们不需要返回值
  const mountPaths = config.mediaMountPath.filter(s => s);
  for (let i = mountPaths.length - 1; i >= 0; i--) {
    mediaPathMapping.unshift([0, 0, mountPaths[i], ""]);
  }
  let mediaItemPath = util.doUrlMapping(r, mediaServerRes.path, notLocal, mediaPathMapping, "mediaPathMapping");
  // 优化：减少不必要的日志
  // ngx.log(ngx.WARN, `mapped plex file path: ${mediaItemPath}`);

  // strm文件内部远程链接重定向，如：http,rtsp
  // 不仅strm，mediaPathMapping可能使用远程链接
  const isRelative = !util.isAbsolutePath(mediaItemPath);
  if (isRelative) {
    let rule = util.simpleRuleFilter(
      r, config.redirectStrmLastLinkRule, mediaItemPath,
      util.SOURCE_STR_ENUM.filePath, "redirectStrmLastLinkRule"
    );
    if (rule && rule.length > 0) {
      if (!Number.isInteger(rule[0])) {
        r.warn(`convert groupRule remove groupKey and sourceValue`);
        rule = rule.slice(2);
      }
      let directUrl = await ngxExt.fetchLastLink(mediaItemPath, rule[2], rule[3], ua);
      if (directUrl) {
        mediaItemPath = directUrl;
      } else {
        r.warn(`warn: fetchLastLink, not expected result, failback once`);
        directUrl = await ngxExt.fetchLastLink(ngxExt.lastLinkFailback(mediaItemPath), rule[2], rule[3], ua);
        if (directUrl) {
          mediaItemPath = directUrl;
        }
      }
    }
    // 需要小心处理filePathPart的编码，其他部分不要编码
    const filePathPart = urlUtil.getFilePathPart(mediaItemPath);
    if (filePathPart) {
      r.warn(`is CloudDrive/AList link, encodeURIComponent filePathPart before: ${mediaItemPath}`);
      mediaItemPath = mediaItemPath.replace(filePathPart, encodeURIComponent(filePathPart));
    }
    return redirect(r, mediaItemPath);
  }

  // 如果是MS API URL且不是STRM文件，直接处理
  // 但跳过已经在 STRM 处理中失败的情况
  if (!mediaServerRes.skipMsApiCheck && isMsApiUrl(mediaItemPath, msAddr, msPublicAddr)) {
    r.warn(`Direct processing of MS API URL: ${mediaItemPath}`);
    const redirectUrl = await fetchMsApiRedirect(r, mediaItemPath, ua, 30000);
    if (redirectUrl) {
      r.warn(`fetchMsApiRedirect success, redirecting to: ${redirectUrl}`);
      return redirect(r, redirectUrl);
    } else {
      r.warn(`fetchMsApiRedirect failed for direct MS API URL, will fallback to original URL`);
    }
  }

  // 客户端自定义AList规则，在获取AList之前
  const alistDUrl = util.getClientSelfAlistLink(r, mediaItemPath);
  if (alistDUrl) { return redirect(r, alistDUrl); }

  // 获取AList直链
  const alistToken = config.alistToken;
  const alistAddr = config.alistAddr;
  const alistFilePath = mediaItemPath;
  const alistFsGetApiPath = `${alistAddr}/api/fs/get`;
  
  // 使用新的MS API获取直链
  let alistRes;
  let isUsingMsApi = false;
  if (msAddr && msApiKey) {
    const msApiPath = `${msAddr}/api/v1/cloudStorage/strm302?apiKey=${msApiKey}&path=${encodeURIComponent(alistFilePath)}`;
    r.warn(`fetching direct link from MS API: ${msApiPath}`);
    const redirectUrl = await fetchMsApiRedirect(r, msApiPath, ua, 15000);
    if (redirectUrl) {
      alistRes = redirectUrl;
      isUsingMsApi = true;
    } else {
      alistRes = `error: MS API request failed or invalid response`;
    }
  } else {
    // 回退到原来的逻辑
    ngx.log(ngx.WARN, `about to call fetchAlistPathApi with: apiPath=${alistFsGetApiPath}, filePath=${alistFilePath}`);
    alistRes = await util.cost(fetchAlistPathApi,
      alistFsGetApiPath,
      alistFilePath,
      alistToken,
      ua,
    );
  }
  
  // 优化：减少不必要的日志
  // r.warn(`fetchAlistPathApi, UA: ${ua}`);
  // r.warn(`fetchAlistPathApi result: ${alistRes}`);
  if (!alistRes.startsWith("error")) {
    // 路由规则，代理模式下只检查alistRes
    // r.warn(`checking route mode for alistRes: ${alistRes}`);
    const routeMode = util.getRouteMode(r, alistRes, true, notLocal);
    // r.warn(`routeMode result: ${routeMode}`);
    if (util.ROUTE_ENUM.proxy === routeMode) {
      return internalRedirect(r); // 使用原始链接
    }
    // 客户端自定义AList规则，在获取AList之后，覆盖raw_url
    // 如果是使用MS API获取的链接，则跳过clientSelfAlistRule处理
    let redirectUrl = isUsingMsApi ? alistRes : (util.getClientSelfAlistLink(r, alistRes, alistFilePath) ?? alistRes);
    // r.warn(`getClientSelfAlistLink result: ${redirectUrl}`);
    const key = "alistRawUrlMapping";
    if (config[key] && config[key].length > 0) {
      const mappedUrl = util.doUrlMapping(r, redirectUrl, notLocal, config[key], key);
      if (mappedUrl) {
        redirectUrl = mappedUrl;
        // 优化：减少不必要的日志
        // ngx.log(ngx.WARN, `${key} mapped: ${redirectUrl}`);
      }
    }
    return redirect(r, redirectUrl);
  }
  r.warn(`alistRes: ${alistRes}`);
  if (alistRes.startsWith("error403")) {
    r.error(`fail to fetch fetchAlistPathApi: ${alistRes},fallback use original link`);
    return fallbackUseOriginal ? internalRedirect(r) : r.return(500, alistRes);
  }
  if (alistRes.startsWith("error500")) {
    r.warn(`will req alist /api/fs/list to rerty`);
    // const filePath = alistFilePath.substring(alistFilePath.indexOf("/", 1));
    const filePath = alistFilePath;
    const alistFsListApiPath = `${alistAddr}/api/fs/list`;
    r.warn(`about to call fetchAlistPathApi for fs/list: apiPath=${alistFsListApiPath}, filePath=/`);
    const foldersRes = await fetchAlistPathApi(
      alistFsListApiPath,
      "/",
      alistToken,
      ua,
    );
    r.warn(`fetchAlistPathApi fs/list result: ${foldersRes}`);
    if (foldersRes.startsWith("error")) {
      r.error(`fail to fetch /api/fs/list: ${foldersRes},fallback use original link`);
      return fallbackUseOriginal ? internalRedirect(r) : r.return(500, foldersRes);
    }
    const folders = foldersRes.split(",").sort();
    for (let i = 0; i < folders.length; i++) {
      r.warn(`try to fetch alist path from /${folders[i]}${filePath}`);
      let driverRes = await fetchAlistPathApi(
        alistFsGetApiPath,
        `/${folders[i]}${filePath}`,
        alistToken,
        ua,
      );
      if (!driverRes.startsWith("error")) {
        driverRes = driverRes.includes("http://172.17.0.1")
          ? driverRes.replace("http://172.17.0.1", config.alistPublicAddr)
          : driverRes;
        return redirect(r, driverRes);
      }
    }
    r.error(`fail to fetch alist resource: not found,fallback use original link`);
    return fallbackUseOriginal ? internalRedirect(r) : r.return(500, "fail to fetch alist resource");
  }
  r.error(`fail to fetch fetchAlistPathApi: ${alistRes},fallback use original link`);
  return fallbackUseOriginal ? internalRedirect(r) : r.return(500, alistRes);
}

/**
 * 检查是否允许重定向
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @returns {Boolean} 是否允许重定向
 */
function allowRedirect(r) {
  const redirectConfig = config.redirectConfig;
  if (!redirectConfig) {
    return true;
  }
  if (!redirectConfig.enable) {
    r.warn(`redirectConfig.enable: ${redirectConfig.enable}`);
    return false;
  }
  const apiType = r.variables.apiType ?? "";
  r.warn(`apiType: ${apiType}, redirectConfig: ${JSON.stringify(redirectConfig)}`);
  const enableMap = {
    PartStreamPlayOrDownload: redirectConfig.enablePartStreamPlayOrDownload,
    VideoTranscodePlay: redirectConfig.enableVideoTranscodePlay,
  };
  return Object.entries(enableMap).some(entry => {
    const key = entry[0];
    const value = entry[1];
    return value && (apiType.endsWith(key) || apiType === key)
  });
}

/**
 * 从AList获取路径API
 * @param {String} alistApiPath - AList API路径
 * @param {String} alistFilePath - 文件路径
 * @param {String} alistToken - AList令牌
 * @param {String} ua - User-Agent
 * @returns {Promise<String>} AList响应结果
 */
// copy from emby2Alist/nginx/conf.d/emby.js
async function fetchAlistPathApi(alistApiPath, alistFilePath, alistToken, ua) {
  const alistRequestBody = {
    path: alistFilePath,
    password: "",
  };
  try {
    const urlParts = urlUtil.parseUrl(alistApiPath);
    const hostValue = `${urlParts.host}:${urlParts.port}`;
    ngx.log(ngx.WARN, `fetchAlistPathApi add Host: ${hostValue}`);
    const response = await ngx.fetch(alistApiPath, {
      method: "POST",
      headers: {
        "Content-Type": "application/json;charset=utf-8",
        Authorization: alistToken,
        "User-Agent": ua,
        Host: hostValue,
      },
      max_response_body_size: 65535,
      timeout: 15000, // 15秒超时
      body: JSON.stringify(alistRequestBody),
    });
    if (response.ok) {
      const result = await response.json();
      if (!result) {
        return `error: alist_path_api response is null`;
      }
      if (result.message == "success") {
        // alist /api/fs/get
        if (result.data.raw_url) {
          return result.data.raw_url;
        }
        // alist /api/fs/link
        if (result.data.header.Cookie) {
          return result.data;
        }
        // alist /api/fs/list
        return result.data.content.map((item) => item.name).join(",");
      }
      if (result.code == 403) {
        return `error403: alist_path_api ${result.message}`;
      }
      return `error500: alist_path_api ${result.code} ${result.message}`;
    } else {
      return `error: alist_path_api ${response.status} ${response.statusText}`;
    }
  } catch (error) {
    return `error: alist_path_api fetchAlistFiled ${error}`;
  }
}

/**
 * 缓存预加载
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} url - 预加载URL
 * @param {String} cacheLevel - 缓存级别
 */
async function cachePreload(r, url, cacheLevel) {
  url = urlUtil.appendUrlArg(url, util.ARGS.cacheLevleKey, cacheLevel);
  ngx.log(ngx.WARN, `cachePreload Level: ${cacheLevel}`);
  // 优化：立即返回，不等待 preload 完成，避免阻塞
  preload(r, url);
  // 注意：preload 是异步的，这里不 await，让它后台执行
}

/**
 * 预加载函数
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} url - 预加载URL
 */
async function preload(r, url) {
  // 优化：减少日志输出，仅在调试时启用
  // events.njsOnExit(`preload`);

  url = urlUtil.appendUrlArg(url, util.ARGS.internalKey, "1");
  const ua = r.headersIn["User-Agent"];
  ngx.fetch(url, {
    method: "HEAD",
    headers: {
      "User-Agent": ua,
    },
    max_response_body_size: 1024
  }).then(res => {
    ngx.log(ngx.WARN, `preload response.status: ${res.status}`);
    if ((res.status > 300 && res.status < 309) || res.status == 200) {
      ngx.log(ngx.WARN, `success: preload used UA: ${ua}, url: ${url}`);
    } else {
      ngx.log(ngx.WARN, `error: preload, skip`);
    }
  }).catch((error) => {
    ngx.log(ngx.ERR, `error: preload: ${error}`);
  });
}

// plex专用函数

/**
 * 从Plex获取文件路径
 * @param {String} itemInfoUri - 项目信息URI
 * @param {Number} mediaIndex - 媒体索引
 * @param {Number} partIndex - 部分索引
 * @returns {Promise<Object>} 媒体服务器响应结果
 */
async function fetchPlexFilePath(itemInfoUri, mediaIndex, partIndex) {
  let rvt = {
    message: "success",
    path: "",
    media: null,
  };
  try {
    const res = await ngx.fetch(itemInfoUri, {
      method: "GET",
      headers: {
      	"Accept": "application/json", // 只有Plex需要这个
        "Content-Type": "application/json;charset=utf-8",
        "Content-Length": 0,
      },
      max_response_body_size: 65535,
      timeout: 180000 // 180秒超时
    });
    if (res.ok) {
      const result = await res.json();
      if (!result) {
        rvt.message = `error: plex_api itemInfoUri response is null`;
        return rvt;
      }
      if (!result.MediaContainer.Metadata) {
        rvt.message = `error: plex_api not found search results`;
        return rvt;
      }
      // location ~* /library/parts/(\d+)/(\d+)/file, 没有mediaIndex和partIndex
      mediaIndex = mediaIndex ? mediaIndex : 0;
      partIndex = partIndex ? partIndex : 0;
      const media = result.MediaContainer.Metadata[0].Media[mediaIndex];
      rvt.path = media.Part[partIndex].file;
      rvt.media = media;
      return rvt;
    } else {
      rvt.message = `error: plex_api ${res.status} ${res.statusText}`;
      return rvt;
    }
  } catch (error) {
    rvt.message = `error: plex_api fetch mediaItemInfo failed, ${error}`;
    return rvt;
  }
}

/**
 * 获取Plex项目信息
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @returns {Object} Plex项目信息
 */
async function getPlexItemInfo(r) {
  const plexHost = config.plexHost;
  const path = r.args.path;
  const mediaIndex = r.args.mediaIndex;
  const partIndex = r.args.partIndex;
  const api_key = r.args[util.ARGS.plexTokenKey];
  let filePath;
  let itemInfoUri = "";
  if (path) {
	  // 参见：location ~* /video/:/transcode/universal/start
  	itemInfoUri = `${plexHost}${path}?${util.ARGS.plexTokenKey}=${api_key}`;
  } else {
  	// 参见：location ~* /library/parts/(\d+)/(\d+)/file
    filePath = ngx.shared.partInfoDict.get(r.uri);
    r.warn(`getPlexItemInfo r.uri: ${r.uri}`);
    if (!filePath) {
      r.warn(`!!! not expect, will fallback search`);
      const plexRes = await fetchPlexFileFullName(`${plexHost}${r.uri}?download=1&${util.ARGS.plexTokenKey}=${api_key}`);
      if (!plexRes.startsWith("error")) {
        const plexFileName = plexRes.substring(0, plexRes.lastIndexOf("."));
        itemInfoUri = `${plexHost}/search?query=${encodeURIComponent(plexFileName)}&${util.ARGS.plexTokenKey}=${api_key}`;
      } else {
        r.warn(plexRes);
      }
    }
  }
  return { filePath, itemInfoUri, mediaIndex, partIndex, api_key };
}

/**
 * 获取Plex文件全名
 * @param {String} downloadApiPath - 下载API路径
 * @returns {Promise<String>} Plex文件全名或错误信息
 */
async function fetchPlexFileFullName(downloadApiPath) {
  try {
    const response = await ngx.fetch(downloadApiPath, {
      method: "HEAD",
      max_response_body_size: 100 * 1024 ** 3, // 100GB,not important,because HEAD method not have body
    });
    if (response.ok) {
      return util.getFileNameByHead(decodeURI(response.headers["Content-Disposition"]));
    } else {
      return `error: plex_download_api ${response.status} ${response.statusText}`;
    }
  } catch (error) {
    return `error: plex_download_api fetchPlexFileNameFiled ${error}`;
  }
}

/**
 * 获取STRM内部文本
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @returns {Promise<String>} STRM内部文本或错误信息
 */
async function fetchStrmInnerText(r) {
  const plexHost = config.plexHost;
  const api_key = r.args[util.ARGS.plexTokenKey] || r.headersIn[util.ARGS.plexTokenKey];
  const downloadApiPath = `${plexHost}${r.uri}?download=1&${util.ARGS.plexTokenKey}=${api_key}`;
  try {
  	// fetch Api忽略nginx位置
    const response = await ngx.fetch(downloadApiPath, {
      method: "HEAD",
      max_response_body_size: 1024,
      timeout: 30000 // 30秒超时
    });
    // plex strm downloadApi自己返回301，response.redirected api错误返回false
    if (response.status > 300 && response.status < 309) {
      const location = response.headers["Location"];
      // 对于STRM文件，我们直接返回重定向位置，不做任何修改
      // 因为它可能指向完全不同的服务（如AList、CloudDrive等）
      r.log(`fetchStrmInnerText: ${location}，decodeURI(location): ${decodeURI(location)}`);
      return decodeURI(location);
    }
    if (response.ok) {
      r.log(`fetchStrmInnerText: ${response.text()}`);
      return decodeURI(response.text());
    } else {
      return `error: plex_download_api ${response.status} ${response.statusText}`;
    }
  } catch (error) {
    return `error: plex_download_api fetchStrmInnerText ${error}`;
  }
}

/**
 * Plex API处理器
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 */
async function plexApiHandler(r) {
  // 优化：减少日志输出，仅在调试时启用
  // events.njsOnExit(`plexApiHandler: ${r.uri}`);

  const apiType = r.variables.apiType ?? "";
  if (!config.transcodeConfig.enable && apiType === "TranscodeUniversalDecision") {
    // 默认修改直接播放支持所有为true
    r.variables.request_uri += "&directPlay=1&directStream=1";
    r.headersOut["X-Modify-DirectPlay-Success"] = true;
  }
  const subR = await r.subrequest(urlUtil.proxyUri(r.uri), {
    method: r.method,
  });
  const contentType = subR.headersOut["Content-Type"];
  r.log(`plexApiHandler Content-Type Header: ${contentType}`);
  let bodyObj;
  let sBody;
  if (subR.status === 200) {
    if (contentType.includes("application/json")) {
      bodyObj = JSON.parse(subR.responseText);
      plexApiHandlerForJson(r, bodyObj);
      sBody = JSON.stringify(bodyObj);
    } else if (contentType.includes("text/xml")) {
      bodyObj = xml.parse(subR.responseText);
      plexApiHandlerForXml(r, bodyObj);
      sBody = xml.serialize(bodyObj);
    }
  } else {
  	r.warn(`plexApiHandler subrequest failed, status: ${subR.status}`);
	  return internalRedirect(r);
  }

  util.copyHeaders(subR.headersOut, r.headersOut);
  return r.return(200, sBody);
}

/**
 * Plex API JSON处理器
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {Object} body - 响应体
 */
function plexApiHandlerForJson(r, body) {
  const mediaContainer = body.MediaContainer;
  mediaContainerHandler(r, mediaContainer);
  const directoryArr = mediaContainer.Directory;
  if (directoryArr) {
    // 优化：使用 for 循环替代 map，性能更好
    for (let i = 0; i < directoryArr.length; i++) {
      directoryHandler(r, directoryArr[i]);
    }
  }
  if (mediaContainer.size > 0) {
    let metadataArr = [];
    if (mediaContainer.Hub) {
      // 优化：使用 for 循环替代 map，性能更好
      for (let i = 0; i < mediaContainer.Hub.length; i++) {
        const hub = mediaContainer.Hub[i];
        if (hub.Metadata) {
          for (let j = 0; j < hub.Metadata.length; j++) {
            metadataArr.push(hub.Metadata[j]);
          }
        }
      }
    } else {
      if (mediaContainer.Metadata) {
        // 优化：直接使用数组，避免不必要的 push
        metadataArr = mediaContainer.Metadata;
      }
    }
    // 优化：使用 for 循环替代 map，性能更好
    for (let i = 0; i < metadataArr.length; i++) {
      const metadata = metadataArr[i];
      metadataHandler(r, metadata);
      if (metadata.Media) {
        for (let j = 0; j < metadata.Media.length; j++) {
          const media = metadata.Media[j];
          mediaInfoHandler(r, media);
          if (media.Part) {
            for (let k = 0; k < media.Part.length; k++) {
              partInfoHandler(r, media.Part[k]);
            }
          }
        }
      }
    }
  }
}

/**
 * Plex API XML处理器
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {Object} body - 响应体
 */
function plexApiHandlerForXml(r, body) {
  const mediaContainerXmlDoc = body.MediaContainer;
  mediaContainerHandler(r, mediaContainerXmlDoc, true);
  const directoryXmlDoc = mediaContainerXmlDoc.$tags$Directory;
  if (directoryXmlDoc) {
    // 优化：使用 for 循环替代 map，性能更好
    for (let i = 0; i < directoryXmlDoc.length; i++) {
      directoryHandler(r, directoryXmlDoc[i], true);
    }
  }
  let videoXmlNodeArr = mediaContainerXmlDoc.$tags$Video;
  let mediaXmlNodeArr;
  let partXmlNodeArr;
  // r.log(videoXmlNodeArr.length);
  if (videoXmlNodeArr && videoXmlNodeArr.length > 0) {
    // 优化：使用 for 循环替代 map，性能更好
    for (let i = 0; i < videoXmlNodeArr.length; i++) {
      const video = videoXmlNodeArr[i];
      metadataHandler(r, video, true);
      // Video.key禁止修改，客户端不支持
      mediaXmlNodeArr = video.$tags$Media;
      if (mediaXmlNodeArr && mediaXmlNodeArr.length > 0) {
        for (let j = 0; j < mediaXmlNodeArr.length; j++) {
          const media = mediaXmlNodeArr[j];
          mediaInfoHandler(r, media, true);
          partXmlNodeArr = media.$tags$Part;
          if (partXmlNodeArr && partXmlNodeArr.length > 0) {
            for (let k = 0; k < partXmlNodeArr.length; k++) {
              partInfoHandler(r, partXmlNodeArr[k], true);
            }
          }
        }
      }
    }
  }
}

/**
 * 处理器设计模式，从根到子的顺序
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {Object} mainObject - 不同的单个对象
 * @param {Boolean} isXmlNode - mainObject是否为xml节点
 */

function mediaContainerHandler(r, mediaContainer, isXmlNode) {
  // 其他自定义处理
}

function directoryHandler(r, directory, isXmlNode) {
  // modifyDirectoryHidden(r, directory, isXmlNode);
  // 其他自定义处理
}

function metadataHandler(r, metadata, isXmlNode) {
  // Metadata.key禁止修改，客户端不支持
  // json是metadata，xml是$tags$Video标签
  // 其他自定义处理
}

function mediaInfoHandler(r, media, isXmlNode) {
  fillMediaInfo(r, media, isXmlNode);
  // 其他自定义处理
}

function partInfoHandler(r, part, isXmlNode) {
  cachePartInfo(r, part, isXmlNode);
  fillPartInfo(r, part, isXmlNode);
  // 其他自定义处理
}

// 其他自定义处理

function cachePartInfo(r, part, isXmlNode) {
  if (!part) return;
  // Part.key可以修改，但某些客户端不支持
  // partKey += `?${util.filePathKey}=${partFilePath}`;
  let partKey = part.key;
  let partFilePath = part.file;
  if (isXmlNode) {
    partKey = part.$attr$key;
    partFilePath = part.$attr$file;
  }
  util.dictAdd("partInfoDict", partKey, partFilePath);
  routeCachePartInfo(r, partKey);
}

function routeCachePartInfo(r, partKey) {
  if (!partKey) return;
  if (config.routeCacheConfig.enableL2
    && r.uri.startsWith("/library/metadata")) {
    // 优化：立即执行预加载，不等待，避免阻塞主流程
    // 使用立即执行的异步函数，确保不阻塞当前处理
    (async function() {
      try {
        await cachePreload(r, urlUtil.getCurrentRequestUrlPrefix(r) + partKey, util.CHCHE_LEVEL_ENUM.L2);
      } catch (error) {
        // 静默处理错误，不影响主流程
        r.log(`cachePreload error: ${error}`);
      }
    })();
  }
}

function fillMediaInfo(r, media, isXmlNode) {
  if (!media) return;
  // 只有strm文件没有mediaContainer
  // 没有真正的容器也可以播放，但字幕可能出错
  const defaultContainer = "mp4";
  if (isXmlNode) {
    if (!media.$attr$container) {
      media.$attr$container = defaultContainer;
    }
  } else {
    if (!media.container) {
      media.container = defaultContainer;
    }
  }
}

function fillPartInfo(r, part, isXmlNode) {
  if (!part) return;
  // 只有strm文件没有mediaContainer
  // 没有真正的容器也可以播放，但字幕可能出错
  const defaultContainer = "mp4";
  const defaultStream = [];
  const isInfuse = r.headersIn["User-Agent"].includes("Infuse");
  if (isXmlNode) {
    if (!part.$attr$container) {
      part.$attr$container = defaultContainer;
    }
    if (!part.$attr$Stream) {
      part.$attr$Stream = defaultStream;
    }
    if (isInfuse && part.$attr$file.toLowerCase().endsWith(".strm")) {
      part.$attr$file = part.$attr$file + `.${defaultContainer}`;
    }
  } else {
    if (!part.container) {
      part.container = defaultContainer;
    }
    if (!part.Stream) {
      part.Stream = defaultStream;
    }
    if (isInfuse && part.file.toLowerCase().endsWith(".strm")) {
      part.file = part.file + `.${defaultContainer}`;
    }
  }
}

/** @deprecated: 废弃 */
function modifyDirectoryHidden(r, dir, isXmlNode) {
  if (!dir) return;
  if (isXmlNode) {
    if (dir.$attr$hidden == "2") {
      dir.$attr$hidden = "0";
    }
  } else {
    if (dir.hidden == 2) {
      dir.hidden = 0;
    }
  }
  r.warn(`${dir.title}, modify hidden 2 => 0`);
}

// js_header_filter指令用于调试测试
// function libraryStreams(r) {
//   events.njsOnExit(`libraryStreams: ${r.uri}`);

//   // let cl = r.headersOut["Content-Length"];
//   // if (Array.isArray(cl)) {
//   //   r.warn(`upstream sent duplicate header line: "Content-Length: ${JSON.stringify(cl)} "`);
//   //   cl = cl.pop();
//   // }
//   r.warn(`libraryStreams headersIn: ${JSON.stringify(r.headersIn)}`);
//   r.warn(`libraryStreams headersOut: ${JSON.stringify(r.headersOut)}`);
// }

/**
 * 重定向后处理
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} url - 重定向URL
 * @param {String} cachedRouteDictKey - 缓存路由字典键
 */
async function redirectAfter(r, url, cachedRouteDictKey) {
  try {
    // 优化：移除不必要的延迟，直接执行缓存操作
    let cachedMsg = "";
    const routeCacheConfig = config.routeCacheConfig;
    if (routeCacheConfig.enable) {
      const ua = r.headersIn["User-Agent"];
      // webClient下载只有itemId在pathParam中
      let cacheKey = util.parseExpression(r, routeCacheConfig.keyExpression) ?? r.uri;
      const domainArr115 = config.strHead["115"];
      const uaIsolation = Array.isArray(domainArr115) ? domainArr115.some(d => url.includes(d)) : url.includes(domainArr115);
      cacheKey = uaIsolation ? `${cacheKey}:${ua}` : cacheKey;
      r.log(`redirectAfter cacheKey: ${cacheKey}`);
      // cachePreload在URL中添加了参数
      const cacheLevle = r.args[util.ARGS.cacheLevleKey] ?? util.CHCHE_LEVEL_ENUM.L1;
      let flag = !ngx.shared["routeL2Dict"].has(cacheKey);
        // && !ngx.shared["routeL3Dict"].has(cacheKey);
      let routeDictKey = "routeL1Dict";
      if (util.CHCHE_LEVEL_ENUM.L2 === cacheLevle) {
        routeDictKey = "routeL2Dict";
        flag = !ngx.shared["routeL1Dict"].has(cacheKey);
      // } else if (util.CHCHE_LEVEL_ENUM.L3 === cacheLevle) {
      //   routeDictKey = "routeL3Dict";
      //   flag = !ngx.shared["routeL1Dict"].has(cacheKey) && !ngx.shared["routeL2Dict"].has(cacheKey);
      }
      if (flag) {
        util.dictAdd(routeDictKey, cacheKey, url);
        cachedMsg += `cache ${routeDictKey} added, `;
      }
      cachedMsg = cachedRouteDictKey ? `hit cache ${cachedRouteDictKey}, ` : cachedMsg;
    }
  } catch (error) {
    r.error(`error: redirectAfter: ${error}`);
  }
}

/**
 * 内部重定向后处理
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} uri - 重定向URI
 * @param {String} cachedRouteDictKey - 缓存路由字典键
 */
async function internalRedirectAfter(r, uri, cachedRouteDictKey) {
  try {
    const routeCacheConfig = config.routeCacheConfig;
    if (routeCacheConfig.enable) {
      const cacheKey = util.parseExpression(r, routeCacheConfig.keyExpression) ?? r.uri;
      util.dictAdd("routeL1Dict", cacheKey, uri);
    }
  } catch (error) {
    r.error(`error: internalRedirectAfter: ${error}`);
  }
}

/**
 * 重定向函数
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} url - 重定向URL
 * @param {String} cachedRouteDictKey - 缓存路由字典键
 */
async function redirect(r, url, cachedRouteDictKey) {
  // 对于strm，只有Plex需要这个，如part位置，但配置不使用add_header，重复的："null *"
  // add_header Access-Control-Allow-Origin *;
  r.headersOut["Access-Control-Allow-Origin"] = "*";

  if (config.alistSignEnable) {
    const originalUrl = url;
    url = util.addAlistSign(url, config.alistToken, config.alistSignExpireTime);
    r.warn(`addAlistSign: ${originalUrl} => ${url}`);
  }
  if (config.redirectCheckEnable && !(await util.cost(ngxExt.linkCheck, url, r.headersIn["User-Agent"]))) {
    r.warn(`redirectCheck fail: ${url}`);
    return internalRedirect(r);
  }

  r.warn(`redirect to: ${url}`);
  // 优化：减少不必要的日志输出，仅在调试时启用
  // r.warn(`redirect request headers: ${JSON.stringify(r.headersIn)}`);
  // r.warn(`redirect request args: ${JSON.stringify(r.args)}`);
  // 需要调用者：return;
  r.return(302, url);

  // 异步
  redirectAfter(r, url, cachedRouteDictKey);
}

/**
 * 内部重定向函数
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} uri - 重定向URI
 * @param {String} cachedRouteDictKey - 缓存路由字典键
 */
function internalRedirect(r, uri, cachedRouteDictKey) {
  if (!uri) {
    uri = "@root";
    r.warn(`use original link`);
  }
  r.log(`internalRedirect to: ${uri}`);
  // 需要调用者：return;
  r.internalRedirect(uri);

  // 异步
  internalRedirectAfter(r, uri, cachedRouteDictKey);
}

/**
 * 内部重定向期望函数
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 * @param {String} uri - 重定向URI
 */
function internalRedirectExpect(r, uri) {
  if (!uri) { uri = "@root"; }
  r.log(`internalRedirect to: ${uri}`);
  // 需要调用者：return;
  r.internalRedirect(uri);
}

/**
 * 屏蔽后处理
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 */
async function blockedAfter(r) {
  try {
    // 优化：移除不必要的延迟，直接执行日志记录
    const xMedia = r[util.ARGS.rXMediaKey];
    const msg = [
      "blocked",
      `uri: ${r.uri}`,
      `remote_addr: ${r.variables.remote_addr}`,
      `headersIn: ${JSON.stringify(r.headersIn)}`,
      `args: ${JSON.stringify(r.args)}`,
      `mediaPartFile: ${xMedia.Part[0].file}`
    ].join('\n');
    r.warn(`blocked: ${msg}`);
  } catch (error) {
    r.error(`error: blockedAfter: ${error}`);
  }
}

/**
 * 屏蔽函数
 * @param {Object} r - Nginx JavaScript对象，代表HTTP请求
 */
function blocked(r) {
  // 需要调用者：return;
  r.return(403, "blocked");
  // 异步
  blockedAfter(r);
}

export default {
  redirect2Pan,
  fetchPlexFilePath,
  plexApiHandler,
  redirect,
  internalRedirect,
  internalRedirectExpect,
  blocked,
};
