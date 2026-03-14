# 🏛️ 한일 보험 규제 챗봇

**일본·한국 보험 규제 비교 AI** — 보험사, 보험대리점, 인슈어테크, 컴플라이언스 담당자를 위한 전문 챗봇

🔗 **서비스 URL**: https://seetheskyeric.github.io/Japan-Insurance-Law-Chatbot/

---

## 기능

| 기능 | 설명 |
|---|---|
| 💬 AI 챗봇 | 한일 보험 규제 자유 질문 · 즉시 비교 분석 |
| 📚 법령 조회 | 51개 법령을 영역별 2열 비교 (JP/KR) |
| 📄 원문 + 번역 | 일본 법령 조문 원문 + 한글 자동 번역 |
| 🔍 RAG 검색 | Gemini 임베딩 기반 벡터 유사도 검색 |

---

## 아키텍처

```
GitHub Pages (docs/)          Vercel (api/chat.js)         Supabase
┌──────────────┐    POST      ┌─────────────────┐    RPC    ┌──────────────┐
│  index.html  │ ──────────▶  │  Claude Haiku   │ ────────▶ │ chunks 테이블 │
│  (챗봇 UI)   │              │  + RAG 검색     │           │ (임베딩 벡터)  │
└──────────────┘              └─────────────────┘           └──────────────┘
                                       │
                               Gemini text-embedding-004
```

---

## 수집 법령 목록 (51개)

### 🇯🇵 일본 (26개)

| 영역 | 법령 |
|---|---|
| 보험업 기본 | 保険法, 保険業法, 保険業法施行令, 保険業法施行規則 |
| 보험업 감독규정 | 保険会社向け監督指針, 少額短期保険業者向け監督指針, 認可特定保険業者向け監督指針 |
| 소비자보호 | 金融サービス提供法, 金融サービス提供法施行令 |
| 금융투자·자본시장 | 金融商品取引法, 金融商品取引法施行令, 金融商品取引業者向け監督指針, 金融サービス仲介業者向け監督指針 |
| 의무보험·특별법 | 自動車損害賠償保障法(令), 地震保険法(令) |
| 퇴직연금·방카슈랑스 | 確定拠出年金法, 確定給付企業年金法, 銀行法 |
| 감독 일반 | 事務ガイドライン第三分冊, 告示·ガイドライン·Q&A |
| 세법 | 所得税法, 相続税法, 法人税法 |

### 🇰🇷 한국 (25개)

| 영역 | 법령 |
|---|---|
| 보험업 기본 | 상법(보험편), 보험업법, 보험업법 시행령, 보험업법 시행규칙 |
| 보험업 감독규정 | 보험업감독규정, 보험업감독업무시행세칙 |
| 소비자보호 | 금융소비자보호법, 금융소비자보호법 시행령, 금융소비자보호감독규정, 금융소비자보호감독규정시행세칙 |
| 금융투자·자본시장 | 자본시장법, 자본시장법 시행령, 금융투자업규정 |
| 의무보험·특별법 | 자동차손해배상보장법(령), 화재재해보상보험법 |
| 퇴직연금·방카슈랑스 | 근로자퇴직급여보장법(령), 은행법, 우체국예금·보험에 관한 법률 |
| 감독 일반 | 금융기관검사및제재에관한규정 |
| 세법 | 소득세법, 상속세및증여세법, 법인세법 |

---

## 기술 스택

| 구성요소 | 기술 |
|---|---|
| Frontend | HTML/CSS/JS (GitHub Pages) |
| API Proxy | Vercel Serverless (Node.js) |
| LLM | Claude Haiku (`claude-haiku-4-5-20251001`) |
| 임베딩 | Gemini `text-embedding-004` (768차원) |
| 벡터DB | Supabase pgvector |
| 파이프라인 | GitHub Actions (월 1회 자동 수집) |
| JP 법령 수집 | e-Gov 법령 API |
| KR 법령 수집 | 법제처 국가법령정보센터 API |
| KR 행정규칙 수집 | 법제처 행정규칙 API |
| FSA 감독지침 | 금융청 웹사이트 크롤링 |

---

## 로컬 실행

```bash
# 의존성 설치
pip install requests supabase google-generativeai beautifulsoup4

# 환경변수 설정
export SUPABASE_URL="..."
export SUPABASE_SERVICE_KEY="..."
export GEMINI_API_KEY="..."
export HOUREI_API_KEY="..."  # 법제처 API OC 코드

# 전체 법령 수집 + 임베딩
python scripts/pipeline.py --country ALL --phase 0

# 감독지침·행정규칙 수집
python scripts/crawler.py --target ALL

# 임베딩만 채우기 (법령 재수집 없이)
python scripts/pipeline.py --embed-only
```

---

## GitHub Actions 수동 실행 옵션

| 옵션 | 설명 |
|---|---|
| `country` | ALL / JP / KR |
| `phase` | 0=전체, 1=핵심법령, 2=특별법·연금, 3=세법 |
| `run_crawler` | FSA 감독지침 + 행정규칙 크롤링 여부 |
| `embed_only` | 법령 재수집 없이 임베딩만 채우기 |

---

## 프로젝트 구조

```
.
├── .github/workflows/
│   └── update_laws.yml      # 월 1회 자동 수집 파이프라인
├── api/
│   └── chat.js              # Vercel 프록시 (RAG + Claude)
├── docs/
│   ├── index.html           # 챗봇 UI (GitHub Pages)
│   └── data/
│       └── laws_index.json  # 법령 메타데이터
├── scripts/
│   ├── pipeline.py          # JP/KR 법령 수집 + 임베딩
│   └── crawler.py           # FSA 감독지침 + 행정규칙 수집
└── requirements.txt
```

---

*Habitfactory × Signal Financial Lab*
