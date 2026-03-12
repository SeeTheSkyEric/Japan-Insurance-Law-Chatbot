// Vercel Serverless Function
// 역할: ① Supabase 키워드 검색 → ② Gemini 답변 생성

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GEMINI_KEY   = process.env.GEMINI_API_KEY;

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

    // ── ② Gemini 호출 ────────────────────────────────────────────────────────
    const lastMessages = [...messages];
    // 마지막 user 메시지에 법령 컨텍스트 주입
    if (contextChunks && lastMessages.length > 0) {
      const last = lastMessages[lastMessages.length - 1];
      if (last.role === "user") {
        lastMessages[lastMessages.length - 1] = {
          ...last,
          content: last.content + contextChunks,
        };
      }
    }

    const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key=${GEMINI_KEY}`;
    const body = {
      system_instruction: { parts: [{ text: system }] },
      contents: lastMessages.map(m => ({
        role: m.role === "assistant" ? "model" : "user",
        parts: [{ text: m.content }],
      })),
      generationConfig: {
        temperature: 0.3,
        maxOutputTokens: 4000,
      },
    };

    const gRes = await fetch(geminiUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const data = await gRes.json();
    if (!gRes.ok) {
      console.error("Gemini 오류:", JSON.stringify(data));
      // Gemini 에러도 200으로 반환 (클라이언트가 받을 수 있도록)
      return res.status(200).json({ text: `Gemini 오류 (${gRes.status}): ${data?.error?.message || JSON.stringify(data)}` });
    }

    const text = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
    return res.status(200).json({ text, hasContext: !!contextChunks });

  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
