"""
한일 보험 법령 파이프라인 (수정판)
- e-Gov API: XML 파싱으로 변경
- 올바른 법령 코드 적용
실행: python scripts/pipeline.py --country JP
      python scripts/pipeline.py --country KR
      python scripts/pipeline.py --country ALL
"""

import os, re, time, json, logging, argparse
import xml.etree.ElementTree as ET
import requests
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── 환경 변수 ──────────────────────────────────────────────────────────────────
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
HOUREI_API_KEY = os.environ.get("HOUREI_API_KEY", "")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── 법령 정의 (법령번호 = e-Gov 실제 코드) ────────────────────────────────────
JP_LAWS = [
    {"id":"JP-01","law_code":"420AC0000000056", "name_ja":"保険法",         "name_ko":"보험법",           "category":["보험사","소비자보호"]},
    {"id":"JP-02","law_code":"407AC0000000105", "name_ja":"保険業法",        "name_ko":"보험업법",          "category":["보험사","허가·감독","보험대리점"]},
    {"id":"JP-05","law_code":"", "name_ja":"金融サービスの提供及び利用環境の整備等に関する法律","name_ko":"금융서비스제공법","category":["소비자보호","인슈어테크"]},  # TODO: law_code 확인 필요
    {"id":"JP-07","law_code":"323AC0000000025", "name_ja":"金融商品取引法",  "name_ko":"금융상품거래법",    "category":["인슈어테크","소비자보호"]},
]

KR_LAWS = [
    {"id":"KR-01","law_code":"1000",  "name_ja":"", "name_ko":"상법",           "category":["보험사","소비자보호"]},
    {"id":"KR-02","law_code":"1739",  "name_ja":"", "name_ko":"보험업법",        "category":["보험사","허가·감독","보험대리점"]},
    {"id":"KR-07","law_code":"17799", "name_ja":"", "name_ko":"금융소비자보호법", "category":["소비자보호","보험대리점"]},
    {"id":"KR-11","law_code":"8635",  "name_ja":"", "name_ko":"자본시장법",      "category":["인슈어테크","소비자보호"]},
]

# ── e-Gov API (일본, XML 파싱) ─────────────────────────────────────────────────
EGOV_BASE = "https://laws.e-gov.go.jp/api/1"

def fetch_jp_law(law: dict) -> list[dict]:
    url = f"{EGOV_BASE}/lawdata/{law['law_code']}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    # e-Gov는 XML 반환
    root = ET.fromstring(r.text)
    ns = {"": ""}  # 네임스페이스 없음

    articles = []
    # Article 태그 전체 탐색
    for art in root.iter("Article"):
        num   = art.findtext("ArticleTitle") or ""
        title = art.findtext("ArticleCaption") or ""
        # 조문 전체 텍스트 추출
        text  = " ".join(t.strip() for t in art.itertext() if t.strip())
        if not text or len(text) < 10:
            continue
        articles.append({
            "id":       f"{law['id']}-{re.sub(r'[^0-9]', '', num) or num}",
            "law_id":   law["id"],
            "article":  num,
            "title":    title,
            "text":     text[:2000],  # 최대 2000자
            "keywords": _extract_keywords(text),
            "category": law["category"],
        })
    log.info(f"  JP {law['id']}: {len(articles)}개 조문 수집")
    return articles

# ── 법제처 OpenAPI (한국) ──────────────────────────────────────────────────────
HOUREI_BASE = "https://www.law.go.kr/DRF/lawService.do"

def fetch_kr_law(law: dict) -> list[dict]:
    params = {
        "OC":     HOUREI_API_KEY,
        "target": "law",
        "type":   "JSON",
        "MST":    law["law_code"],   # MST = 법령ID (ID 대신 MST 사용)
    }
    r = requests.get(HOUREI_BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    articles = []
    law_data = data.get("법령", {})
    jo_list  = law_data.get("조문", {})

    # 단일 조문인 경우 리스트로 통일
    if isinstance(jo_list, dict):
        jo_list = jo_list.get("조문단위", [])
    if isinstance(jo_list, dict):
        jo_list = [jo_list]

    for art in jo_list:
        num   = art.get("조문번호", "")
        title = art.get("조문제목", "")
        text  = art.get("조문내용", "")
        # 항(項) 내용도 합치기
        hang = art.get("항", [])
        if isinstance(hang, dict): hang = [hang]
        for h in hang:
            text += " " + h.get("항내용", "")
        if not text.strip():
            continue
        articles.append({
            "id":       f"{law['id']}-art{re.sub(r'[^0-9]', '', str(num))}",
            "law_id":   law["id"],
            "article":  str(num),
            "title":    title,
            "text":     text.strip()[:2000],
            "keywords": _extract_keywords(text),
            "category": law["category"],
        })
    log.info(f"  KR {law['id']}: {len(articles)}개 조문 수집")
    return articles

# ── 청크 분할 ──────────────────────────────────────────────────────────────────
def split_chunks(articles: list[dict]) -> list[dict]:
    chunks = []
    for art in articles:
        if len(art["text"]) / 2 <= 500:
            chunks.append(art)
        else:
            parts = re.split(r'(?=①|②|③|④|⑤|⑥|⑦|⑧|⑨|⑩)', art["text"])
            for i, para in enumerate(parts):
                if para.strip():
                    chunks.append({**art, "id": f"{art['id']}-p{i+1}", "text": para.strip()})
    log.info(f"  청크 분할: {len(articles)}개 → {len(chunks)}개")
    return chunks

# ── 임베딩 생성 ────────────────────────────────────────────────────────────────
EMBED_MODEL = "text-embedding-004"
BATCH_SIZE  = 50
GEMINI_EMBED_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent?key={key}"

def embed_single(text: str) -> list[float]:
    """Gemini REST API로 임베딩 생성 (SDK 없이 직접 호출)"""
    url = GEMINI_EMBED_URL.format(model=EMBED_MODEL, key=GEMINI_API_KEY)
    body = {
        "model": f"models/{EMBED_MODEL}",
        "content": {"parts": [{"text": text}]},
        "taskType": "RETRIEVAL_DOCUMENT",
    }
    r = requests.post(url, json=body, timeout=30)
    r.raise_for_status()
    return r.json()["embedding"]["values"]

def embed_chunks(chunks: list[dict]) -> list[dict]:
    for i, c in enumerate(chunks):
        try:
            text = f"{c['title']} {c['text']}"
            c["embedding"] = embed_single(text)
            if (i + 1) % 10 == 0:
                log.info(f"  임베딩: {i+1}/{len(chunks)}")
        except Exception as e:
            log.warning(f"  임베딩 실패 ({c['id']}), 스킵: {e}")
            c["embedding"] = None  # 실패해도 계속 진행
        time.sleep(0.5)
    return chunks

# ── Supabase UPSERT ────────────────────────────────────────────────────────────
def upsert_laws(law_list: list[dict], country: str):
    rows = [{
        "id": l["id"], "name_ja": l["name_ja"], "name_ko": l["name_ko"],
        "country": country, "category": l["category"],
        "law_code": l["law_code"], "phase": 1,
    } for l in law_list]
    supabase.table("laws").upsert(rows).execute()
    log.info(f"  laws upsert: {len(rows)}건")

def upsert_chunks(chunks: list[dict]):
    rows = [{
        "id": c["id"], "law_id": c["law_id"], "article": c["article"],
        "title": c["title"], "text": c["text"],
        "keywords": c["keywords"], "category": c["category"],
        "embedding": c["embedding"],
    } for c in chunks if c.get("embedding") is not None]

    # embedding 없는 청크는 별도로 저장 (embedding 컬럼 제외)
    rows_no_emb = [{
        "id": c["id"], "law_id": c["law_id"], "article": c["article"],
        "title": c["title"], "text": c["text"],
        "keywords": c["keywords"], "category": c["category"],
    } for c in chunks if c.get("embedding") is None]
    if rows_no_emb:
        for i in range(0, len(rows_no_emb), 50):
            supabase.table("chunks").upsert(rows_no_emb[i:i+50]).execute()
        log.info(f"  embedding 없는 청크 저장: {len(rows_no_emb)}건")
    for i in range(0, len(rows), 50):
        supabase.table("chunks").upsert(rows[i:i+50]).execute()
        log.info(f"  chunks upsert: {min(i+50, len(rows))}/{len(rows)}")

# ── laws_index.json 생성 ───────────────────────────────────────────────────────
def export_laws_index(law_list: list[dict], country: str):
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
    log.info("  laws_index.json 갱신 완료")

# ── 유틸 ───────────────────────────────────────────────────────────────────────
def _extract_keywords(text: str) -> list[str]:
    words = re.findall(r'[ぁ-んァ-ン一-龥가-힣]{3,}', text)
    freq  = {}
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
            if not law["law_code"]:
                log.warning(f"  {law['id']} 스킵: law_code 미확인")
                continue
            try:
                articles = fetch_jp_law(law)
                all_chunks.extend(split_chunks(articles))
            except Exception as e:
                log.error(f"  {law['id']} 수집 실패: {e}")
        if all_chunks:
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
        if all_chunks:
            all_chunks = embed_chunks(all_chunks)
            upsert_chunks(all_chunks)
        export_laws_index(KR_LAWS, "KR")

    log.info("=== 완료 ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", choices=["JP","KR","ALL"], default="ALL")
    args = parser.parse_args()
    run(args.country)
