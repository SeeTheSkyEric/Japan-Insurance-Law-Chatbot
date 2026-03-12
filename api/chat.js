// Vercel Serverless Function
// 역할: ① Supabase 키워드 검색 → ② Claude Haiku 답변 생성

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const CLAUDE_KEY   = process.env.CLAUDE_API_KEY;

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const { messages, system, query } = req.body;

  try {
    // ── ① Supabase 키워드 검색 ───────────────────────────────────────────────
    let contextChunks = "";
    if (query && SUPABASE_URL && SUPABASE_KEY) {
      const words = query.replace(/[？?！!。、]/g, " ").split(/\s+/).filter(w => w.length >= 2);
      const orFilter = words.map(w => `text.ilike.%${encodeURIComponent(w)}%`).join(",");
      const sbUrl = `${SUPABASE_URL}/rest/v1/chunks?or=(${orFilter})&select=law_id,article,title,text&limit=6`;
      try {
        const sbRes = await fetch(sbUrl, {
          headers: {
            apikey: SUPABASE_KEY,
            Authorization: `Bearer ${SUPABASE_KEY}`,
          }
        });
        const chunks = await sbRes.json();
        if (Array.isArray(chunks) && chunks.length > 0) {
          contextChunks = "\n\n[참조 법령 조문]\n" + chunks.map(c =>
            `[${c.law_id} ${c.article}] ${c.title}\n${c.text?.slice(0, 300)}`
          ).join("\n\n");
        }
      } catch (e) {
        console.error("Supabase 검색 실패:", e.message);
      }
    }

    // ── ② 마지막 user 메시지에 법령 컨텍스트 주입 ────────────────────────────
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

    // ── ③ Claude Haiku API 호출 ───────────────────────────────────────────────
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
        text: `Claude 오류 (${claudeRes.status}): ${data?.error?.message || JSON.stringify(data)}`
      });
    }

    const text = data.content?.[0]?.text || "응답을 받지 못했습니다.";
    return res.status(200).json({ text, hasContext: !!contextChunks });

  } catch (e) {
    console.error("서버 오류:", e.message);
    return res.status(200).json({ text: `서버 오류: ${e.message}` });
  }
};
