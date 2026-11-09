// https://platform.minimax.cn/docs/api-reference/speech-t2a-http
// Prices are configured by the administrator in USD per 10,000 characters.
export const meta = {
  apiVersion: 1,
  key: "minimax-tts",
  name: "MiniMax TTS",
  icon: "Minimax.Color",
  version: "1.0.2",
  author: { name: "QuantumNous" },
  description: { en: "MiniMax speech synthesis with character billing", zh: "MiniMax 按字符计费的语音合成" },
  baseUrl: "https://api.minimax.cn",
  models: ["speech-2.8-turbo", "speech-2.8-hd", "speech-2.6-turbo", "speech-2.6-hd", "speech-02-turbo", "speech-02-hd", "speech-01-turbo", "speech-01-hd"],
  fetchMode: "per_task",
  protocols: [{ name: "openai_speech", supports: ["sync"] }],
  usageSchema: {
    characters: {
      type: "number",
      unit: "character",
      description: { en: "Speech synthesis unit price", zh: "语音合成单价" },
    },
  },
  usageExamples: [{ label: "10,000 characters", facts: { characters: 10000 } }],
};

// Counts Unicode code points, not UTF-16 code units or UTF-8 bytes. Chinese
// punctuation is not Han. This is a reservation estimate; vendor usage wins.
function billableCharacters(text) {
  if (typeof text !== "string" || !text.trim()) throw new Error("input must be a non-empty string");
  let length = 0;
  let characters = 0;
  for (const ch of text) {
    if (++length >= 10000) throw new Error("input must contain fewer than 10000 Unicode characters");
    const codePoint = ch.codePointAt(0);
    if (codePoint >= 0xd800 && codePoint <= 0xdfff) throw new Error("input contains an invalid Unicode character");
    characters += /\p{Script=Han}/u.test(ch) ? 2 : 1;
  }
  return characters;
}

function speechRequest(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("JSON object required");
  for (const key of Object.keys(body)) {
    if (!["model", "input", "voice", "speed", "response_format", "stream_format", "stream", "background", "metadata"].includes(key)) {
      throw new Error("Unsupported speech option: " + key);
    }
  }
  if (body.stream !== undefined && body.stream !== false) throw new Error("MiniMax speech only supports synchronous audio");
  if (body.background !== undefined && body.background !== false) throw new Error("Background speech is not supported");
  if (body.stream_format !== undefined && body.stream_format !== "audio") throw new Error("stream_format must be audio");
  if (body.response_format !== undefined && body.response_format !== "mp3") throw new Error("response_format must be mp3");
  // No metadata passthrough: it must not change text/model/output or supply
  // client-controlled billing facts after validation and reservation.
  if (
    body.metadata !== undefined &&
    (!body.metadata || typeof body.metadata !== "object" || Array.isArray(body.metadata) || Object.keys(body.metadata).length)
  ) {
    throw new Error("MiniMax speech metadata overrides are not supported");
  }
  billableCharacters(body.input);
  if (typeof body.voice !== "string" || !body.voice.trim() || body.voice.length > 256) throw new Error("voice must be a MiniMax voice ID");
  const speed = body.speed === undefined ? 1 : body.speed;
  if (typeof speed !== "number" || !Number.isFinite(speed) || speed < 0.5 || speed > 2) throw new Error("speed must be between 0.5 and 2");
  return { input: body.input, voice: body.voice.trim(), speed: speed };
}

export const protocols = {
  openai_speech: {
    decodeRequest(ctx) {
      if (!ctx.body || ctx.body.kind !== "json") throw new Error("JSON body required");
      if (ctx.stream) throw new Error("Streaming speech is not supported");
      return { kind: "submit", model: ctx.model, action: "text_to_speech", requestBody: speechRequest(ctx.body.value) };
    },
    render(_ctx, task) {
      if (task.status !== "SUCCESS") throw new Error("Speech is not ready");
      return { artifactKey: "audio" };
    },
  },
};

export function buildSubmitRequest(ctx) {
  const body = speechRequest(ctx.requestBody);
  const model = ctx.upstreamModel || ctx.model;
  if (!meta.models.includes(model)) throw new Error("Unsupported MiniMax speech model");
  return {
    url: ctx.baseUrl.replace(/\/+$/, "") + "/v1/t2a_v2",
    method: "POST",
    // For api_key auth the host's authHeader is the raw key, not a Bearer header.
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + ctx.apiKey },
    body: {
      model: model,
      text: body.input,
      stream: false,
      output_format: "url",
      voice_setting: { voice_id: body.voice, speed: body.speed, vol: 1, pitch: 0 },
      audio_setting: { format: "mp3", sample_rate: 32000, bitrate: 128000, channel: 1 },
    },
  };
}

export function extractUsage(ctx) {
  if (ctx.usagePurpose === "billing_ratios") return null;
  return { characters: billableCharacters(speechRequest(ctx.requestBody).input) };
}

export function parseSubmitResponse(ctx, response) {
  const body = response.body;
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new Error("MiniMax returned an invalid JSON response; check the channel Base URL");
  }
  const base = body.base_resp;
  if (!base || typeof base !== "object" || Array.isArray(base)) {
    throw new Error("MiniMax response is missing base_resp; the channel must use the MiniMax /v1/t2a_v2 API");
  }
  const code = base.status_code;
  if (!Number.isSafeInteger(code) || code < 0) throw new Error("MiniMax returned an invalid base_resp.status_code");
  if (code !== 0) {
    // Hook errors reach API callers. Do not copy arbitrary status_msg, trace_id
    // or response bodies, which can echo credentials, input text or signed URLs.
    // https://platform.minimax.cn/docs/api-reference/errorcode
    const reasons = {
      1000: "Unknown upstream error",
      1001: "Request timed out",
      1002: "Rate limit exceeded",
      1004: "Authentication failed; check the channel API key and MiniMax API region",
      1008: "Insufficient MiniMax account balance",
      1024: "Internal upstream error",
      1026: "Input rejected by content moderation",
      1027: "Output rejected by content moderation",
      1033: "Upstream service error",
      1042: "Too many invisible or invalid input characters",
      1045: "Task concurrency limit exceeded",
      2013: "Invalid request parameters",
      20132: "Invalid voice_id or voice sample",
      2014: "Empty input",
      2022: "Model unavailable or access denied",
      2042: "Access to voice_id denied",
      2043: "The model does not support this voice_id",
      2049: "Invalid MiniMax API key",
      2053: "Insufficient MiniMax credits",
      2054: "voice_id does not exist",
      2061: "The current plan does not support this model",
      2064: "Upstream service is busy",
      2066: "Request processing timed out",
      2067: "Plan quota exhausted and automatic credit consumption is disabled",
      2068: "The current plan tier does not support this model",
      2069: "The model has been retired",
    };
    throw new Error("MiniMax speech synthesis failed (status_code=" + code + "): " + (reasons[code] || "See the MiniMax error code documentation"));
  }
  const data = body.data;
  const extra = body.extra_info || {};
  if (!data || data.status !== 2 || typeof data.audio !== "string" || !/^https:\/\/[^\s]+$/.test(data.audio) || data.audio.length > 65536) {
    throw new Error("MiniMax returned no completed audio URL");
  }
  if (extra.audio_format !== undefined && extra.audio_format !== "mp3") throw new Error("MiniMax returned an unexpected audio format");
  const estimated = billableCharacters(speechRequest(ctx.requestBody).input);
  const usage = extra.usage_characters;
  const validUsage = typeof usage === "number" && Number.isInteger(usage) && usage >= 0 && usage <= 19998;
  return {
    taskId: ctx.publicTaskId,
    taskData: { audio_url: data.audio, audio_format: "mp3", usage_characters: validUsage ? usage : null },
    state: {
      estimated_characters: estimated,
      reported_characters: validUsage ? usage : null,
      usage_source: validUsage ? "upstream" : "estimate_missing_or_invalid",
      counting_rule: "han-double-codepoint-v1",
      trace_id: typeof body.trace_id === "string" ? body.trace_id.slice(0, 256) : "",
    },
    immediate: { status: "SUCCESS", progress: "100%" },
  };
}

export function extractUsageOnComplete(_ctx, _result, data) {
  const usage = data && data.usage_characters;
  if (typeof usage !== "number" || !Number.isInteger(usage) || usage < 0 || usage > 19998) return null;
  return { characters: usage };
}

export function buildQueryRequest() {
  throw new Error("Synchronous speech has no remote task to poll");
}

export function parseTaskResult() {
  throw new Error("Synchronous speech has no remote task to poll");
}

export function listArtifacts(task) {
  if (task.status !== "SUCCESS" || !task.data || !task.data.audio_url) return [];
  return [{ key: "audio", type: "audio", mimeType: "audio/mpeg" }];
}

export function buildContentRequest(ctx) {
  if (ctx.artifactKey !== "audio" || !ctx.data || !ctx.data.audio_url) throw new Error("Speech audio is unavailable");
  return { url: ctx.data.audio_url, method: "GET", credentialless: true };
}
