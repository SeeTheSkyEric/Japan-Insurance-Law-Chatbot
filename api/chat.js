// Vercel Serverless Function
// 역할: ① Gemini 임베딩 생성 → ② Supabase 벡터 검색 → ③ Claude Haiku 답변 생성

const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const CLAUDE_KEY    = process.env.CLAUDE_API_KEY;
const GEMINI_KEY    = process.env.GEMINI_API_KEY;

// ── 질문 텍스트 → Gemini text-embedding-004 벡터 생성 ──────────────────────
async function embedQuery(text) {
  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1/models/text-embedding-004:embedContent?key=${GEMINI_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: "models/text-embedding-004",
        content: { parts: [{ text }] },
      }),
    }
  );
  const data = await res.json();
  if (!res.ok) throw new Error(`Gemini 임베딩 실패: ${JSON.stringify(data)}`);
  return data.embedding.values; // float[] 768차원
}

// ── Supabase search_chunks RPC 호출 ────────────────────────────────────────
async function searchChunks(embedding, countryFilter = null, matchCount = 6) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/search_chunks`, {
    method: "POST",
    headers: {
      "Content-Type":  "application/json",
      apikey:          SUPABASE_KEY,
      Authorization:   `Bearer ${SUPABASE_KEY}`,
    },
    body: JSON.stringify({
      query_embedding: embedding,
      country_filter:  countryFilter,
      match_count:     matchCount,
    }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Supabase 검색 실패: ${JSON.stringify(data)}`);
  return Array.isArray(data) ? data : [];
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST")    return res.status(405).json({ error: "Method not allowed" });

  const { messages, system, query } = req.body;

  try {
    // ── ① 벡터 검색으로 관련 조문 가져오기 ────────────────────────────────
    let contextChunks = "";
    let ragError = null;
    if (query && SUPABASE_URL && SUPABASE_KEY && GEMINI_KEY) {
      try {
        const embedding = await embedQuery(query);
        const chunks    = await searchChunks(embedding, null, 6);

        if (chunks.length > 0) {
          contextChunks = "\n\n[참조 법령 조문]\n" + chunks.map(c =>
            `[${c.country} ${c.law_id} ${c.article}${c.title ? ` ${c.title}` : ""}] (유사도: ${(c.similarity * 100).toFixed(1)}%)\n${c.text?.slice(0, 400)}`
          ).join("\n\n");
        }
      } catch (e) {
        console.error("RAG 검색 실패, 키워드 폴백:", e.message);
        ragError = e.message;

        // ── 폴백: 임베딩 실패 시 기존 키워드 검색으로 대체 ──────────────
        try {
          const words    = query.replace(/[？?！!。、]/g, " ").split(/\s+/).filter(w => w.length >= 2);
          const orFilter = words.map(w => `text.ilike.%${encodeURIComponent(w)}%`).join(",");
          const sbUrl    = `${SUPABASE_URL}/rest/v1/chunks?or=(${orFilter})&select=law_id,article,title,text&limit=6`;
          const sbRes    = await fetch(sbUrl, {
            headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` },
          });
          const kwChunks = await sbRes.json();
          if (Array.isArray(kwChunks) && kwChunks.length > 0) {
            contextChunks = "\n\n[참조 법령 조문]\n" + kwChunks.map(c =>
              `[${c.law_id} ${c.article}] ${c.title}\n${c.text?.slice(0, 300)}`
            ).join("\n\n");
          }
        } catch (e2) {
          console.error("키워드 폴백도 실패:", e2.message);
        }
      }
    }

    // ── ② 마지막 user 메시지에 법령 컨텍스트 주입 ──────────────────────────
    const lastMessages = [...messages];
    if (contextChunks && lastMessages.length > 0) {
      const last = lastMessages[lastMessages.length - 1];
      if (last.role === "user") {
        lastMessages[lastMessages.length - 1] = {
          ...last,
          content: last.content + contextChunks,
        };
      }
    }

    // ── ③ Claude Haiku API 호출 ─────────────────────────────────────────────
    const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type":      "application/json",
        "x-api-key":         CLAUDE_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model:      "claude-haiku-4-5-20251001",
        max_tokens: 4000,
        system:     system,
        messages:   lastMessages,
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
    return res.status(200).json({ text, hasContext: !!contextChunks, ragError: ragError || undefined });

  } catch (e) {
    console.error("서버 오류:", e.message);
    return res.status(200).json({ text: `서버 오류: ${e.message}` });
  }
};
