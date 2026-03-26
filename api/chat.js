// ============================================================
// 한일 보험법 챗봇 v2 — Vercel Serverless API (스트리밍 버전)
// ============================================================

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const CLAUDE_KEY   = process.env.CLAUDE_API_KEY;
const VOYAGE_KEY   = process.env.VOYAGE_API_KEY;

async function embedQuery(text) {
  const res = await fetch("https://api.voyageai.com/v1/embeddings", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${VOYAGE_KEY}`,
    },
    body: JSON.stringify({ input: [text], model: "voyage-law-2", input_type: "query" }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Voyage 임베딩 실패: ${JSON.stringify(data)}`);
  return data.data[0].embedding;
}

async function searchChunks(embedding, countryFilter = null, matchCount = 6) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/search_chunks_v2`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_KEY,
      "Authorization": `Bearer ${SUPABASE_KEY}`,
    },
    body: JSON.stringify({
      query_embedding: embedding,
      country_filter: countryFilter,
      match_count: matchCount,
      similarity_threshold: 0.25,
    }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Supabase 검색 실패: ${JSON.stringify(data)}`);
  return Array.isArray(data) ? data : [];
}

function formatChunks(chunks, country) {
  if (!chunks.length) return "";
  const flag = country === "JP" ? "🇯🇵" : "🇰🇷";
  return chunks.map(c => {
    const ref = `[${c.law_id} ${c.article_num}${c.article_title ? ` ${c.article_title}` : ""}]`;
    const text = country === "JP" ? (c.text_ja || c.text_ko || "") : (c.text_ko || c.text_ja || "");
    return `${flag} ${ref} (유사도: ${(c.similarity * 100).toFixed(1)}%)\n${text.slice(0, 500)}`;
  }).join("\n\n");
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const { messages, system, query } = req.body;
  const stream = req.body.stream !== false; // 기본값 true

  // ── ① 벡터 검색 ─────────────────────────────────────────────
  let jpContext = "", krContext = "", ragError = null;

  if (query && VOYAGE_KEY && SUPABASE_URL) {
    try {
      const embedding = await embedQuery(query);
      const [jpChunks, krChunks] = await Promise.all([
        searchChunks(embedding, "JP", 6),
        searchChunks(embedding, "KR", 6),
      ]);
      jpContext = formatChunks(jpChunks, "JP");
      krContext = formatChunks(krChunks, "KR");
    } catch (e) {
      console.error("RAG 검색 실패:", e.message);
      ragError = e.message;
    }
  }

  // ── ② 컨텍스트 주입 ──────────────────────────────────────────
  const lastMessages = [...messages];
  if ((jpContext || krContext) && lastMessages.length > 0) {
    const last = lastMessages[lastMessages.length - 1];
    if (last.role === "user") {
      let ctx = "\n\n[참조 법령 조문]";
      if (jpContext) ctx += `\n\n=== 일본 관련 조문 ===\n${jpContext}`;
      if (krContext) ctx += `\n\n=== 한국 관련 조문 ===\n${krContext}`;
      lastMessages[lastMessages.length - 1] = { ...last, content: last.content + ctx };
    }
  }

  // ── ③ Claude 스트리밍 호출 ───────────────────────────────────
  if (!stream) {
    // 비스트리밍 (번역 등 내부 호출용)
    const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": CLAUDE_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        max_tokens: 2000,
        system,
        messages: lastMessages,
      }),
    });
    const data = await claudeRes.json();
    if (!claudeRes.ok) return res.status(200).json({ text: `오류: ${data?.error?.message}` });
    return res.status(200).json({ text: data.content?.[0]?.text || "" });
  }

  // 스트리밍 응답 헤더
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");

  const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": CLAUDE_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-6",
      max_tokens: 2000,
      stream: true,
      system,
      messages: lastMessages,
    }),
  });

  if (!claudeRes.ok) {
    const err = await claudeRes.json();
    res.write(`data: ${JSON.stringify({ error: err?.error?.message || "Claude 오류" })}\n\n`);
    return res.end();
  }

  // SSE 스트림 파싱 → 클라이언트로 전달
  const reader = claudeRes.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop(); // 마지막 불완전 라인 보존

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const data = line.slice(6).trim();
        if (data === "[DONE]") continue;
        try {
          const evt = JSON.parse(data);
          if (evt.type === "content_block_delta" && evt.delta?.type === "text_delta") {
            res.write(`data: ${JSON.stringify({ text: evt.delta.text })}\n\n`);
          }
          if (evt.type === "message_stop") {
            res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
          }
        } catch {}
      }
    }
  } catch (e) {
    console.error("스트림 오류:", e.message);
  }

  res.end();
};
