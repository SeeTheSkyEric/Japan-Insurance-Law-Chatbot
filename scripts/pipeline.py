"""
한일 보험 법령 파이프라인
실행: python pipeline.py --country JP  /  --country KR  /  --country ALL
"""

import os, re, time, json, logging, argparse
import requests
from supabase import create_client
import google.generativeai as genai

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── 환경 변수 ──────────────────────────────────────────────────────────────────
SUPABASE_URL    = os.environ["SUPABASE_URL"]
SUPABASE_KEY    = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY  = os.environ["GEMINI_API_KEY"]
HOUREI_API_KEY  = os.environ.get("HOUREI_API_KEY", "")  # 법제처 API 키

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)

# ── 법령 정의 (1단계) ──────────────────────────────────────────────────────────
JP_LAWS = [
    {"id":"JP-01","law_code":"4061","name_ja":"保険法",          "name_ko":"보험법",           "category":["보험사","소비자보호"]},
    {"id":"JP-02","law_code":"7174","name_ja":"保険業法",         "name_ko":"보험업법",          "category":["보험사","허가·감독","보험대리점"]},
    {"id":"JP-03","law_code":"32767","name_ja":"保険業法施行令",   "name_ko":"보험업법 시행령",   "category":["보험사","보험대리점"]},
    {"id":"JP-04","law_code":"32768","name_ja":"保険業法施行規則", "name_ko":"보험업법 시행규칙", "category":["보험사","보험대리점"]},
    {"id":"JP-05","law_code":"5187","name_ja":"金融サービスの提供及び利用環境の整備等に関する法律","name_ko":"금융서비스제공법","category":["소비자보호","인슈어테크"]},
    {"id":"JP-07","law_code":"5170","name_ja":"金融商品取引法",   "name_ko":"금융상품거래법",    "category":["인슈어테크","소비자보호"]},
]

KR_LAWS = [
    {"id":"KR-01","law_code":"109071","name_ja":"","name_ko":"상법 제4편 보험",     "category":["보험사","소비자보호"]},
    {"id":"KR-02","law_code":"109072","name_ja":"","name_ko":"보험업법",             "category":["보험사","허가·감독","보험대리점"]},
    {"id":"KR-03","law_code":"109073","name_ja":"","name_ko":"보험업법 시행령",      "category":["보험사","보험대리점"]},
    {"id":"KR-04","law_code":"109074","name_ja":"","name_ko":"보험업법 시행규칙",    "category":["보험사","보험대리점"]},
    {"id":"KR-07","law_code":"109075","name_ja":"","name_ko":"금융소비자보호법",     "category":["소비자보호","보험대리점"]},
    {"id":"KR-11","law_code":"109076","name_ja":"","name_ko":"자본시장법",           "category":["인슈어테크","소비자보호"]},
]

# ── e-Gov API (일본 법령) ──────────────────────────────────────────────────────
EGOV_BASE = "https://laws.e-gov.go.jp/api/1"

def fetch_jp_law(law: dict) -> list[dict]:
    """e-Gov API에서 법령 원문을 가져와 조문 단위로 반환"""
    url = f"{EGOV_BASE}/lawdata/{law['law_code']}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    articles = []
    for art in data.get("law_full_text", {}).get("law_body", {}).get("main_provision", {}).get("articles", []):
        num   = art.get("article_num", "")
        title = art.get("article_caption", "")
        text  = _flatten_text(art)
        if not text.strip():
            continue
        articles.append({
            "id":       f"{law['id']}-{num.replace(' ','')}",
            "law_id":   law["id"],
            "article":  num,
            "title":    title,
            "text":     text,
            "keywords": _extract_keywords(text),
            "category": law["category"],
        })
    log.info(f"  JP {law['id']}: {len(articles)}개 조문 수집")
    return articles

# ── 법제처 OpenAPI (한국 법령) ─────────────────────────────────────────────────
HOUREI_BASE = "https://www.law.go.kr/DRF/lawService.do"

def fetch_kr_law(law: dict) -> list[dict]:
    """법제처 OpenAPI에서 법령 조문 수집"""
    params = {
        "OC": HOUREI_API_KEY,
        "target": "law",
        "type": "JSON",
        "ID": law["law_code"],
    }
    r = requests.get(HOUREI_BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    articles = []
    for art in data.get("법령", {}).get("조문", {}).get("조문단위", []):
        num   = art.get("조문번호", "")
        title = art.get("조문제목", "")
        text  = art.get("조문내용", "")
        if not text.strip():
            continue
        articles.append({
            "id":       f"{law['id']}-art{num.replace('제','').replace('조','')}",
            "law_id":   law["id"],
            "article":  num,
            "title":    title,
            "text":     text,
            "keywords": _extract_keywords(text),
            "category": law["category"],
        })
    log.info(f"  KR {law['id']}: {len(articles)}개 조문 수집")
    return articles

# ── 청크 분할 ──────────────────────────────────────────────────────────────────
MAX_TOKENS = 500   # 목표 청크 크기 (토큰 근사치 = 글자수 / 2)

def split_chunks(articles: list[dict]) -> list[dict]:
    """500 토큰 초과 조문은 항(項) 단위로 분할"""
    chunks = []
    for art in articles:
        if len(art["text"]) / 2 <= MAX_TOKENS:
            chunks.append(art)
        else:
            # 항(①②③ 또는 1. 2.) 단위로 분할
            paragraphs = re.split(r'(?=①|②|③|④|⑤|⑥|⑦|⑧|⑨|⑩|１\.|２\.)', art["text"])
            for i, para in enumerate(paragraphs):
                if not para.strip():
                    continue
                chunks.append({
                    **art,
                    "id":   f"{art['id']}-p{i+1}",
                    "text": para.strip(),
                })
    log.info(f"  청크 분할 완료: {len(articles)}개 조문 → {len(chunks)}개 청크")
    return chunks

# ── 임베딩 생성 ────────────────────────────────────────────────────────────────
EMBED_MODEL = "models/text-embedding-004"
BATCH_SIZE  = 100   # Gemini API 배치 한도

def embed_chunks(chunks: list[dict]) -> list[dict]:
    """Gemini text-embedding-004로 벡터 생성 (배치 처리)"""
    for i in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[i:i+BATCH_SIZE]
        texts = [f"{c['title']} {c['text']}" for c in batch]
        try:
            result = genai.embed_content(
                model=EMBED_MODEL,
                content=texts,
                task_type="RETRIEVAL_DOCUMENT",
            )
            for j, emb in enumerate(result["embedding"]):
                batch[j]["embedding"] = emb
            log.info(f"  임베딩: {i+len(batch)}/{len(chunks)}")
        except Exception as e:
            log.error(f"  임베딩 실패 (배치 {i}): {e}")
            raise
        time.sleep(1)   # Rate limit 방지
    return chunks

# ── Supabase UPSERT ────────────────────────────────────────────────────────────
def upsert_laws(law_list: list[dict], country: str):
    """laws 테이블에 메타데이터 upsert"""
    rows = [{
        "id":       l["id"],
        "name_ja":  l["name_ja"],
        "name_ko":  l["name_ko"],
        "country":  country,
        "category": l["category"],
        "law_code": l["law_code"],
        "phase":    1,
        "updated_at": "NOW()",
    } for l in law_list]
    supabase.table("laws").upsert(rows).execute()
    log.info(f"  laws 테이블 upsert 완료: {len(rows)}건")

def upsert_chunks(chunks: list[dict]):
    """chunks 테이블에 청크 + 벡터 upsert (50개씩 분할)"""
    rows = [{
        "id":        c["id"],
        "law_id":    c["law_id"],
        "article":   c["article"],
        "title":     c["title"],
        "text":      c["text"],
        "keywords":  c["keywords"],
        "category":  c["category"],
        "embedding": c["embedding"],
        "updated_at":"NOW()",
    } for c in chunks if "embedding" in c]

    for i in range(0, len(rows), 50):
        supabase.table("chunks").upsert(rows[i:i+50]).execute()
        log.info(f"  chunks upsert: {i+50}/{len(rows)}")

# ── 법령 목록 JSON (GitHub Pages용) ───────────────────────────────────────────
def export_laws_index(law_list: list[dict], country: str):
    """docs/data/laws_index.json 갱신 (법령 조회 UI용)"""
    path = "docs/data/laws_index.json"
    existing = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = {l["id"]: l for l in json.load(f).get("laws", [])}

    for l in law_list:
        existing[l["id"]] = {
            "id": l["id"], "name_ja": l["name_ja"], "name_ko": l["name_ko"],
            "country": country, "category": l["category"], "phase": 1,
        }

    os.makedirs("docs/data", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"updated_at": time.strftime("%Y-%m-%d"),
                   "laws": list(existing.values())}, f, ensure_ascii=False, indent=2)
    log.info(f"  laws_index.json 갱신 완료")

# ── 유틸 ───────────────────────────────────────────────────────────────────────
def _flatten_text(node: dict) -> str:
    """e-Gov JSON의 중첩 구조에서 텍스트 추출"""
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        return " ".join(_flatten_text(v) for v in node.values())
    if isinstance(node, list):
        return " ".join(_flatten_text(i) for i in node)
    return ""

def _extract_keywords(text: str) -> list[str]:
    """텍스트에서 명사 키워드 간이 추출 (3글자 이상 반복 단어)"""
    words = re.findall(r'[ぁ-んァ-ン一-龥가-힣]{3,}', text)
    freq = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1
    return [w for w, c in sorted(freq.items(), key=lambda x: -x[1]) if c >= 2][:10]

# ── 메인 ───────────────────────────────────────────────────────────────────────
def run(country: str):
    if country in ("JP", "ALL"):
        log.info("=== 일본 법령 수집 시작 ===")
        upsert_laws(JP_LAWS, "JP")
        all_chunks = []
        for law in JP_LAWS:
            try:
                articles = fetch_jp_law(law)
                all_chunks.extend(split_chunks(articles))
            except Exception as e:
                log.error(f"  {law['id']} 수집 실패: {e}")
        all_chunks = embed_chunks(all_chunks)
        upsert_chunks(all_chunks)
        export_laws_index(JP_LAWS, "JP")

    if country in ("KR", "ALL"):
        log.info("=== 한국 법령 수집 시작 ===")
        upsert_laws(KR_LAWS, "KR")
        all_chunks = []
        for law in KR_LAWS:
            try:
                articles = fetch_kr_law(law)
                all_chunks.extend(split_chunks(articles))
            except Exception as e:
                log.error(f"  {law['id']} 수집 실패: {e}")
        all_chunks = embed_chunks(all_chunks)
        upsert_chunks(all_chunks)
        export_laws_index(KR_LAWS, "KR")

    log.info("=== 완료 ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", choices=["JP","KR","ALL"], default="ALL")
    args = parser.parse_args()
    run(args.country)
