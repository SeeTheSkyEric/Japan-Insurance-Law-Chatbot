export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { query, options } = req.body || {};
  if (!query) return res.status(400).json({ error: 'query required' });

  const opts = options || {};
  const sections = [];

  if (opts.overview !== false) sections.push(`## 1. 기본 정보 및 주요 연혁
• 설립연도, 본사 소재지, 대표자, 직원 수
• 상장 여부 (거래소명 / 종목코드)
• 그룹 구조 및 주요 자회사
• 창업~현재 주요 마일스톤 5~8개`);

  if (opts.business !== false) sections.push(`## 2. 사업 현황
• 주요 사업 부문 및 핵심 상품·서비스
• 시장 점유율 및 주요 고객층
• 영업 거점·진출 지역`);

  if (opts.strategy !== false) sections.push(`## 3. 추진 전략
• 중기 경영계획 및 성장 전략
• M&A·제휴·파트너십 동향
• 디지털 전환(DX) 추진 현황`);

  if (opts.finance !== false) sections.push(`## 4. 주요 재무 정보
• 최근 3개 회계연도 매출·영업이익·순이익
• 시가총액 및 주가 현황 (상장사)
• 배당 정책
• 주요 재무 지표 (ROE, 손해율 등 해당 시)`);

  if (opts.insurance !== false) sections.push(`## 5. 보험업 특화 정보
• 보험 인허가 현황 (취급 보험 종류)
• 대리점·모집 채널 구조 및 제휴 현황
• 주요 감독 규제 이슈 (금융청·금융감독원 등)
• 지급여력비율 (솔벤시 마진 비율) 해당 시
• 언더라이팅 또는 상품 특이사항`);

  if (opts.news) sections.push(`## 6. 최근 뉴스 및 이슈
• 최근 6개월 주요 뉴스 및 공시
• 규제 대응, 임원 변동, 시장 이슈`);

  const prompt = `당신은 보험업 전문 기업 분석가입니다.
다음 입력값으로 기업을 특정하세요. 기업명일 수도 있고, 서비스명·상품명·브랜드명일 수도 있습니다.
입력이 서비스명이나 상품명인 경우, 해당 서비스를 운영하는 모기업 또는 운영사를 먼저 특정한 뒤 분석을 진행하세요.

입력값: "${query}"

웹 검색을 활용하여 최신 정보를 수집하고, 아래 항목을 한국어로 작성하세요.
수치·날짜·인명은 구체적으로 기재하고, 불확실한 정보는 "확인 필요"로 표기하세요.
각 항목은 불릿(•)으로 핵심만 간결하게 정리하세요.

${sections.join('\n\n')}

---
마지막 줄: **[정보 출처 및 주의사항]** 한 줄로 요약`;

  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'anthropic-beta': 'web-search-2025-03-05'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4096,
        tools: [{ type: 'web_search_20250305', name: 'web_search' }],
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const data = await response.json();
    if (data.error) return res.status(500).json({ error: data.error.message });

    const text = (data.content || [])
      .filter(b => b.type === 'text')
      .map(b => b.text)
      .join('\n');

    return res.status(200).json({ text });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
