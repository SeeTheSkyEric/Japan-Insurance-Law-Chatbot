// ============================================================
// 한일 보험법 챗봇 v2 — Vercel Serverless API
// 역할: ① Voyage-law-2 임베딩 → ② Supabase v2 벡터 검색 → ③ Claude Sonnet 3단 답변
// ============================================================

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const CLAUDE_KEY   = process.env.CLAUDE_API_KEY;
const VOYAGE_KEY   = process.env.VOYAGE_API_KEY;

// ── Voyage-law-2 임베딩 ──────────────────────────────────────
async function embedQuery(text) {
  const res = await fetch("https://api.voyageai.com/v1/embeddings", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${VOYAGE_KEY}`,
    },
    body: JSON.stringify({
      input: [text],
      model: "voyage-law-2",
      input_type: "query",
    }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Voyage 임베딩 실패: ${JSON.stringify(data)}`);
  return data.data[0].embedding;
}

// ── Supabase search_chunks_v2 RPC 호출 ──────────────────────
async function searchChunks(embedding, countryFilter = null, matchCount = 8) {
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

// ── 컨텍스트 포맷 ────────────────────────────────────────────
function formatChunks(chunks, country) {
  if (!chunks.length) return "";
  const flag = country === "JP" ? "🇯🇵" : "🇰🇷";
  return chunks
    .map(c => {
      const ref = `[${c.law_id} ${c.article_num}${c.article_title ? ` ${c.article_title}` : ""}]`;
      const text = country === "JP"
        ? (c.text_ja || c.text_ko || "")
        : (c.text_ko || c.text_ja || "");
      return `${flag} ${ref} (유사도: ${(c.similarity * 100).toFixed(1)}%)\n${text.slice(0, 600)}`;
    })
    .join("\n\n");
}

// ── 메인 핸들러 ──────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const { messages, system, query } = req.body;

  // ── ① 일본/한국 각각 벡터 검색 ──────────────────────────────
  let jpContext = "";
  let krContext = "";
  let ragError  = null;

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

      // 폴백: 키워드 검색
      try {
        const words = query.replace(/[？?！!。、]/g, " ").split(/\s+/).filter(w => w.length >= 2).slice(0, 3);
        const orFilter = words.map(w => `text_ko.ilike.%${w}%,text_ja.ilike.%${w}%`).join(",");
        const sbRes = await fetch(
          `${SUPABASE_URL}/rest/v1/chunks_v2?or=(${orFilter})&select=law_id,country,article_num,article_title,text_ja,text_ko&limit=8`,
          { headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` } }
        );
        const kw = await sbRes.json();
        if (Array.isArray(kw)) {
          const jp = kw.filter(c => c.country === "JP");
          const kr = kw.filter(c => c.country === "KR");
          jpContext = formatChunks(jp.map(c => ({...c, similarity: 0})), "JP");
          krContext = formatChunks(kr.map(c => ({...c, similarity: 0})), "KR");
        }
      } catch (e2) {
        console.error("키워드 폴백 실패:", e2.message);
      }
    }
  }

  // ── ② 컨텍스트를 마지막 user 메시지에 주입 ──────────────────
  const lastMessages = [...messages];
  if ((jpContext || krContext) && lastMessages.length > 0) {
    const last = lastMessages[lastMessages.length - 1];
    if (last.role === "user") {
      let ctx = "\n\n[참조 법령 조문]";
      if (jpContext) ctx += `\n\n=== 일본 관련 조문 ===\n${jpContext}`;
      if (krContext) ctx += `\n\n=== 한국 관련 조문 ===\n${krContext}`;
      lastMessages[lastMessages.length - 1] = {
        ...last,
        content: last.content + ctx,
      };
    }
  }

  // ── ③ Claude Sonnet 호출 ─────────────────────────────────────
  const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": CLAUDE_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-6",
      max_tokens: 4000,
      system: system,
      messages: lastMessages,
    }),
  });

  const data = await claudeRes.json();
  if (!claudeRes.ok) {
    console.error("Claude 오류:", JSON.stringify(data));
    return res.status(200).json({
      text: `Claude 오류 (${claudeRes.status}): ${data?.error?.message || JSON.stringify(data)}`,
    });
  }

  const text = data.content?.[0]?.text || "응답을 받지 못했습니다.";
  return res.status(200).json({
    text,
    hasContext: !!(jpContext || krContext),
    ragError: ragError || undefined,
  });
};
