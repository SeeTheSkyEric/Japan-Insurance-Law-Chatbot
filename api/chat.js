// ============================================================
// 한일 보험법 챗봇 v2 — Vercel Serverless API
// ============================================================
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const CLAUDE_KEY   = process.env.CLAUDE_API_KEY;
const VOYAGE_KEY   = process.env.VOYAGE_API_KEY;

async function embedQuery(text) {
  const res = await fetch("https://api.voyageai.com/v1/embeddings", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${VOYAGE_KEY}` },
    body: JSON.stringify({ input: [text], model: "voyage-law-2", input_type: "query" }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Voyage 임베딩 실패: ${JSON.stringify(data)}`);
  return data.data[0].embedding;
}

async function searchChunks(embedding, countryFilter = null, matchCount = 8) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/search_chunks_v2`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "apikey": SUPABASE_KEY, "Authorization": `Bearer ${SUPABASE_KEY}` },
    body: JSON.stringify({ query_embedding: embedding, country_filter: countryFilter, match_count: matchCount, similarity_threshold: 0.25 }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Supabase 검색 실패: ${JSON.stringify(data)}`);
  return Array.isArray(data) ? data : [];
}

function formatLawChunks(chunks, country) {
  const flag = country === "JP" ? "🇯🇵" : "🇰🇷";
  return chunks
    .filter(c => !c.source_type || c.source_type === "pdf")
    .map(c => {
      const ref  = `[${c.law_id} ${c.article_num}${c.article_title ? ` ${c.article_title}` : ""}]`;
      const text = country === "JP" ? (c.text_ja || c.text_ko || "") : (c.text_ko || c.text_ja || "");
      return `${flag} ${ref} (유사도: ${(c.similarity * 100).toFixed(1)}%) ${text.slice(0, 500)}`;
    }).filter(Boolean).join("\n\n");
}

function formatCommentaryChunks(chunks, country) {
  const flag = country === "JP" ? "🇯🇵" : "🇰🇷";
  return chunks
    .filter(c => c.source_type === "commentary")
    .map(c => {
      const author = c.author || "출처 미상";
      const ref    = `[${author} 해설${c.article_num ? ` ${c.article_num}` : ""}]`;
      const text   = country === "JP" ? (c.text_ja || c.text_ko || "") : (c.text_ko || c.text_ja || "");
      return `📝 ${flag} ${ref} (유사도: ${(c.similarity * 100).toFixed(1)}%) ${text.slice(0, 500)}`;
    }).filter(Boolean).join("\n\n");
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST")    return res.status(405).json({ error: "Method not allowed" });

  const { messages, system, query } = req.body;

  let jpLaw = "", krLaw = "", jpCom = "", krCom = "", ragError = null;
  if (query && VOYAGE_KEY && SUPABASE_URL) {
    try {
      const embedding = await embedQuery(query);
      const [jpAll, krAll] = await Promise.all([
        searchChunks(embedding, "JP", 8),
        searchChunks(embedding, "KR", 8),
      ]);
      jpLaw = formatLawChunks(jpAll, "JP");
      krLaw = formatLawChunks(krAll, "KR");
      jpCom = formatCommentaryChunks(jpAll, "JP");
      krCom = formatCommentaryChunks(krAll, "KR");
    } catch (e) {
      console.error("RAG 검색 실패:", e.message);
      ragError = e.message;
    }
  }

  const lastMessages = [...messages];
  const hasContext   = !!(jpLaw || krLaw || jpCom || krCom);
  if (hasContext && lastMessages.length > 0) {
    const last = lastMessages[lastMessages.length - 1];
    if (last.role === "user") {
      let ctx = "\n\n[참조 법령 조문 및 해설자료]";
      if (jpLaw) ctx += `\n\n=== 🇯🇵 일본 법령 조문 ===\n${jpLaw}`;
      if (krLaw) ctx += `\n\n=== 🇰🇷 한국 법령 조문 ===\n${krLaw}`;
      if (jpCom) ctx += `\n\n=== 📝 일본 해설·참고자료 ===\n${jpCom}`;
      if (krCom) ctx += `\n\n=== 📝 한국 해설·참고자료 ===\n${krCom}`;
      ctx += "\n\n※ 해설·참고자료를 인용할 때는 반드시 '[저자명의 해설]' 형식으로 출처를 명시하세요.";
      lastMessages[lastMessages.length - 1] = { ...last, content: last.content + ctx };
    }
  }

  const claudeRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": CLAUDE_KEY, "anthropic-version": "2023-06-01" },
    body: JSON.stringify({ model: "claude-sonnet-4-6", max_tokens: 4000, system, messages: lastMessages }),
  });
  const data = await claudeRes.json();
  if (!claudeRes.ok) {
    return res.status(200).json({ text: `오류: ${data?.error?.message || JSON.stringify(data)}` });
  }
  return res.status(200).json({
    text: data.content?.[0]?.text || "응답을 받지 못했습니다.",
    hasContext,
    ragError: ragError || undefined,
  });
};
